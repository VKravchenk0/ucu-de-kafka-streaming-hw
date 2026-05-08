package ua.ucu.de.consumer;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Config {
    private final String bootstrapServers;
    private final String topicName;
    private final int numConsumers;
    private final String outputDir;
    private final String consumerGroup;

    public static Config fromEnvAndArgs(String[] args) {
        ConfigBuilder b = Config.builder()
            .bootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
            .topicName(env("TOPIC_NAME", "frames"))
            .numConsumers(Integer.parseInt(env("NUM_CONSUMERS", "1")))
            .outputDir(env("OUTPUT_DIR", "./output"))
            .consumerGroup(env("CONSUMER_GROUP", "hw2-group"));

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> b.bootstrapServers(args[++i]);
                case "--topic"             -> b.topicName(args[++i]);
                case "--num-consumers"     -> b.numConsumers(Integer.parseInt(args[++i]));
                case "--output-dir"        -> b.outputDir(args[++i]);
                case "--consumer-group"    -> b.consumerGroup(args[++i]);
            }
        }
        return b.build();
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
