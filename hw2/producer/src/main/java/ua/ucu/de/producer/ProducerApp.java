package ua.ucu.de.producer;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerApp {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnvAndArgs(args);
        System.out.printf("Config: bootstrapServers=%s topic=%s partitions=%d producers=%d inputFile=%s outputDir=%s%n",
                cfg.getBootstrapServers(), cfg.getTopicName(), cfg.getNumPartitions(),
                cfg.getNumProducers(), cfg.getInputFile(), cfg.getOutputDir());

        TopicAdmin.ensureTopic(cfg.getBootstrapServers(), cfg.getTopicName(), cfg.getNumPartitions());

        Path outDir = Path.of(cfg.getOutputDir());
        Files.createDirectories(outDir);

        // Queue capacity = 2x number of producers; limits in-flight frames in memory
        BlockingQueue<Frame> queue = new ArrayBlockingQueue<>(cfg.getNumProducers() * 2);

        // Start sender threads
        List<Thread> senderThreads = new ArrayList<>();
        for (int i = 0; i < cfg.getNumProducers(); i++) {
            Thread t = Thread.ofPlatform().name("producer-" + i)
                    .start(new FrameProducer(i, cfg.getBootstrapServers(), cfg.getTopicName(), queue));
            senderThreads.add(t);
        }

        // Grab frames one at a time, convert, enqueue for sending
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(cfg.getInputFile());
             Java2DFrameConverter converter = new Java2DFrameConverter()) {

            grabber.start();
            System.out.printf("Video: %dx%d, %.2f fps, %d frames total%n",
                    grabber.getImageWidth(), grabber.getImageHeight(),
                    grabber.getFrameRate(), grabber.getLengthInFrames());

            org.bytedeco.javacv.Frame avFrame;
            int frameNum = 0;
            while ((avFrame = grabber.grabImage()) != null) {
                BufferedImage image = converter.convert(avFrame);
                if (image == null) continue;

                byte[] jpegBytes = toJpeg(image);

                // Block if senders are busy — avoids loading the whole video into memory
                queue.put(new Frame(frameNum, jpegBytes));
                System.out.printf("[Grabber] Queued frame %d (%d bytes)%n", frameNum, jpegBytes.length);
                frameNum++;
            }
            System.out.printf("[Grabber] Done. Grabbed %d frames.%n", frameNum);
        }

        // Signal end-of-stream: one poison pill is enough; senders re-enqueue it for siblings
        queue.put(FrameProducer.POISON);

        for (Thread t : senderThreads) {
            t.join();
        } 
        System.out.println("All producers finished.");
    }

    private static byte[] toJpeg(BufferedImage image) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ImageIO.write(image, "JPEG", baos);
            return baos.toByteArray();
        }
    }
}
