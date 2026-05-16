package ua.ucu.de.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FrameProducer implements Runnable {
    // Sentinel value signalling end-of-stream to sender threads
    static final Frame POISON = new Frame(-1, null);

    private final int producerId;
    private final String bootstrapServers;
    private final String topicName;
    private final BlockingQueue<Frame> queue;
    private final AtomicInteger sentCount = new AtomicInteger();

    public FrameProducer(int producerId, String bootstrapServers, String topicName,
                         BlockingQueue<Frame> queue) {
        this.producerId = producerId;
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.queue = queue;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            while (true) {
                Frame frame;
                try {
                    frame = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (frame == POISON) {
                    // Put the poison back for other sender threads, then stop
                    queue.offer(POISON);
                    break;
                }

                long sendTime = System.currentTimeMillis();
                byte[] sendTimeBytes = ByteBuffer.allocate(8).putLong(sendTime).array();

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        topicName,
                        String.valueOf(frame.frameNumber()),
                        frame.data()
                );
                record.headers().add(new RecordHeader("send_time_ms", sendTimeBytes));

                producer.send(record);
                int n = sentCount.incrementAndGet();
                System.out.printf("[Producer %d] Sent frame %d (%d bytes)%n",
                        producerId, frame.frameNumber(), frame.data().length);
            }
            producer.flush();
        }
        System.out.printf("[Producer %d] Done. Sent %d frames.%n", producerId, sentCount.get());
    }
}
