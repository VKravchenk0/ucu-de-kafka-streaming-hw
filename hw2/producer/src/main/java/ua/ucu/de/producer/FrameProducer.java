package ua.ucu.de.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public class FrameProducer implements Runnable {
    private final int producerId;
    private final String bootstrapServers;
    private final String topicName;
    private final List<Frame> frames;

    public FrameProducer(int producerId, String bootstrapServers, String topicName, List<Frame> frames) {
        this.producerId = producerId;
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.frames = frames;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            for (Frame frame : frames) {
                long sendTime = System.currentTimeMillis();
                byte[] sendTimeBytes = ByteBuffer.allocate(8).putLong(sendTime).array();

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        topicName,
                        String.valueOf(frame.frameNumber()),
                        frame.data()
                );
                record.headers().add(new RecordHeader("send_time_ms", sendTimeBytes));

                producer.send(record);
                System.out.printf("[Producer %d] Sent frame %d (%d bytes)%n",
                        producerId, frame.frameNumber(), frame.data().length);
            }
            producer.flush();
        }
        System.out.printf("[Producer %d] Done. Sent %d frames.%n", producerId, frames.size());
    }
}
