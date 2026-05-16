package ua.ucu.de.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicAdmin {

    public static void ensureTopic(String bootstrapServers, String topicName, int partitions) throws InterruptedException {
        int maxAttempts = 30;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (AdminClient admin = AdminClient.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"))) {

                Set<String> existing = admin.listTopics().names().get();
                if (!existing.contains(topicName)) {
                    admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1))).all().get();
                    System.out.println("Created topic: " + topicName + " with " + partitions + " partition(s)");
                } else {
                    System.out.println("Topic already exists: " + topicName);
                }
                return;
            } catch (Exception e) {
                System.out.printf("Kafka not ready (attempt %d/%d): %s%n", attempt, maxAttempts, e.getMessage());
                Thread.sleep(3000);
            }
        }
        throw new RuntimeException("Could not connect to Kafka after " + maxAttempts + " attempts");
    }
}
