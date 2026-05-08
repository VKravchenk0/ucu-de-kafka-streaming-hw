package ua.ucu.de.producer;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProducerApp {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnvAndArgs(args);
        System.out.printf("Config: bootstrapServers=%s topic=%s partitions=%d producers=%d inputFile=%s%n",
                cfg.getBootstrapServers(), cfg.getTopicName(), cfg.getNumPartitions(),
                cfg.getNumProducers(), cfg.getInputFile());

        TopicAdmin.ensureTopic(cfg.getBootstrapServers(), cfg.getTopicName(), cfg.getNumPartitions());

        List<ua.ucu.de.producer.Frame> allFrames = extractFrames(cfg.getInputFile());
        System.out.printf("Extracted %d frames from %s%n", allFrames.size(), cfg.getInputFile());

        // Partition frames across producers
        List<List<ua.ucu.de.producer.Frame>> slices = new ArrayList<>();
        for (int i = 0; i < cfg.getNumProducers(); i++) slices.add(new ArrayList<>());
        for (int i = 0; i < allFrames.size(); i++) {
            slices.get(i % cfg.getNumProducers()).add(allFrames.get(i));
        }

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < cfg.getNumProducers(); i++) {
            int id = i;
            Thread t = Thread.ofPlatform().name("producer-" + i)
                    .start(new FrameProducer(id, cfg.getBootstrapServers(), cfg.getTopicName(), slices.get(id)));
            threads.add(t);
        }

        for (Thread t : threads) t.join();
        System.out.println("All producers finished.");
    }

    private static List<ua.ucu.de.producer.Frame> extractFrames(String filePath) throws Exception {
        List<ua.ucu.de.producer.Frame> frames = new ArrayList<>();

        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(filePath);
             Java2DFrameConverter converter = new Java2DFrameConverter()) {

            grabber.start();
            System.out.printf("Video: %dx%d, %.2f fps, %d frames total%n",
                    grabber.getImageWidth(), grabber.getImageHeight(),
                    grabber.getFrameRate(), grabber.getLengthInFrames());

            Frame frame;
            int frameNum = 0;
            while ((frame = grabber.grabImage()) != null) {
                BufferedImage image = converter.convert(frame);
                if (image == null) continue;

                byte[] jpegBytes = toJpeg(image);
                frames.add(new ua.ucu.de.producer.Frame(frameNum++, jpegBytes));
            }
        }
        return frames;
    }

    private static byte[] toJpeg(BufferedImage image) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ImageIO.write(image, "JPEG", baos);
            return baos.toByteArray();
        }
    }
}
