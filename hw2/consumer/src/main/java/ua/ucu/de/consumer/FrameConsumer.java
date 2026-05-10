package ua.ucu.de.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class FrameConsumer implements Runnable {
    
    private static final int PROCESSING_DELAY_MILLIS = 1000;

    private final int consumerId;
    private final Config cfg;
    private final AtomicBoolean running;

    public FrameConsumer(int consumerId, Config cfg, AtomicBoolean running) {
        this.consumerId = consumerId;
        this.cfg = cfg;
        this.running = running;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.getConsumerGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Path outDir = Path.of(cfg.getOutputDir());
        try { Files.createDirectories(outDir); } catch (IOException ignored) {}

        Path csvPath = outDir.resolve("consumer-" + consumerId + ".csv");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
             PrintWriter csv = new PrintWriter(new FileWriter(csvPath.toFile()))) {

            csv.println("frame_number,send_time_ms,receive_time_ms,finish_time_ms,bytes");

            List<String> topics = new ArrayList<>();
            topics.add(cfg.getTopicName());
            consumer.subscribe(topics);

            long idleTimeoutMs = cfg.getExitOnIdleSeconds() * 1000L;
            long lastMessageTime = System.currentTimeMillis();
            boolean hadMessages = false;

            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, byte[]> record : records) {
                    lastMessageTime = System.currentTimeMillis();
                    hadMessages = true;

                    long receiveTime = lastMessageTime;
                    long sendTime = extractSendTime(record);
                    int frameNumber = Integer.parseInt(record.key());
                    int bytes = record.value().length;

                    System.out.printf("[Consumer %d] Processing frame %d (%d bytes)%n",
                            consumerId, frameNumber, bytes);

                    Thread.sleep(PROCESSING_DELAY_MILLIS); // simulate processing
                    long finishTime = System.currentTimeMillis();

                    csv.printf("%d,%d,%d,%d,%d%n",
                            frameNumber, sendTime, receiveTime, finishTime, bytes);
                    csv.flush();
                }

                if (hadMessages && System.currentTimeMillis() - lastMessageTime > idleTimeoutMs) {
                    System.out.printf("[Consumer %d] No messages for %ds, exiting.%n",
                            consumerId, cfg.getExitOnIdleSeconds());
                    running.set(false);
                }
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.printf("[Consumer %d] Stopped.%n", consumerId);
    }

    private long extractSendTime(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader("send_time_ms");
        if (h != null && h.value().length == 8) {
            return ByteBuffer.wrap(h.value()).getLong();
        }
        return System.currentTimeMillis();
    }
}
