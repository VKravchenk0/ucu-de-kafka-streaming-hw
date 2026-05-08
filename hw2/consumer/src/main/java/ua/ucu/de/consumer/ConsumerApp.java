package ua.ucu.de.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerApp {

    public static void main(String[] args) throws InterruptedException {
        Config cfg = Config.fromEnvAndArgs(args);
        System.out.printf("Config: bootstrapServers=%s topic=%s consumers=%d outputDir=%s group=%s%n",
                cfg.getBootstrapServers(), cfg.getTopicName(), cfg.getNumConsumers(),
                cfg.getOutputDir(), cfg.getConsumerGroup());

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumers...");
            running.set(false);
        }));

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < cfg.getNumConsumers(); i++) {
            int id = i;
            Thread t = Thread.ofPlatform().name("consumer-" + i)
                    .start(new FrameConsumer(id, cfg, running));
            threads.add(t);
        }

        for (Thread t : threads) t.join();
    }
}
