package ua.ucu.de.producer;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Config {
    private final String bootstrapServers;
    private final String topicName;
    private final int numPartitions;
    private final int numProducers;
    private final String inputFile;
    private final String outputDir;

    public static Config fromEnvAndArgs(String[] args) {
        ConfigBuilder b = Config.builder()
            .bootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
            .topicName(env("TOPIC_NAME", "frames"))
            .numPartitions(Integer.parseInt(env("NUM_PARTITIONS", "1")))
            .numProducers(Integer.parseInt(env("NUM_PRODUCERS", "1")))
            .inputFile(env("INPUT_FILE", "input.mp4"))
            .outputDir(env("OUTPUT_DIR", "./output"));

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> b.bootstrapServers(args[++i]);
                case "--topic"             -> b.topicName(args[++i]);
                case "--num-partitions"    -> b.numPartitions(Integer.parseInt(args[++i]));
                case "--num-producers"     -> b.numProducers(Integer.parseInt(args[++i]));
                case "--input-file"        -> b.inputFile(args[++i]);
                case "--output-dir"        -> b.outputDir(args[++i]);
            }
        }
        return b.build();
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
