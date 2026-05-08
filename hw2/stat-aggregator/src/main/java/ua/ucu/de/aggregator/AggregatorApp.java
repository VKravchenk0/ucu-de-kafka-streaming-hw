package ua.ucu.de.aggregator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class AggregatorApp {

    record Row(int frameNumber, long sendTimeMs, long receiveTimeMs, long finishTimeMs, long bytes) {}

    public static void main(String[] args) throws IOException {
        String outputDir = getConfig("OUTPUT_DIR", "./output", args, "--output-dir");

        System.out.println("Reading CSVs from: " + outputDir);

        List<Row> rows = new ArrayList<>();
        try (var paths = Files.list(Path.of(outputDir))) {
            paths.filter(p -> p.getFileName().toString().endsWith(".csv"))
                 .forEach(csv -> {
                     try (BufferedReader br = new BufferedReader(new FileReader(csv.toFile()))) {
                         String line;
                         boolean header = true;
                         while ((line = br.readLine()) != null) {
                             if (header) { header = false; continue; }
                             String[] parts = line.split(",");
                             if (parts.length < 5) continue;
                             rows.add(new Row(
                                     Integer.parseInt(parts[0].trim()),
                                     Long.parseLong(parts[1].trim()),
                                     Long.parseLong(parts[2].trim()),
                                     Long.parseLong(parts[3].trim()),
                                     Long.parseLong(parts[4].trim())
                             ));
                         }
                     } catch (IOException e) {
                         System.err.println("Failed to read " + csv + ": " + e.getMessage());
                     }
                 });
        }

        if (rows.isEmpty()) {
            System.out.println("No data found in CSV files.");
            return;
        }

        long minSendTime = rows.stream().mapToLong(Row::sendTimeMs).min().orElseThrow();
        long maxFinishTime = rows.stream().mapToLong(Row::finishTimeMs).max().orElseThrow();
        long totalBytes = rows.stream().mapToLong(Row::bytes).sum();
        long maxLatencyMs = rows.stream().mapToLong(r -> r.finishTimeMs() - r.sendTimeMs()).max().orElseThrow();

        double durationSec = (maxFinishTime - minSendTime) / 1000.0;
        double throughputMbps = (totalBytes * 8.0) / (durationSec * 1_000_000.0);

        String report = String.format("""
                === Kafka Throughput Report ===
                Total frames processed : %d
                Total data             : %,.1f MB
                Duration               : %.2f s
                Throughput             : %.3f Mbps
                Max latency            : %d ms (%.2f s)
                """,
                rows.size(),
                totalBytes / 1_000_000.0,
                durationSec,
                throughputMbps,
                maxLatencyMs,
                maxLatencyMs / 1000.0);

        System.out.println(report);

        Path reportPath = Path.of(outputDir, "report.txt");
        try (PrintWriter pw = new PrintWriter(new FileWriter(reportPath.toFile()))) {
            pw.print(report);
        }
        System.out.println("Report written to: " + reportPath);
    }

    private static String getConfig(String envKey, String defaultValue, String[] args, String argFlag) {
        String v = System.getenv(envKey);
        if (v != null && !v.isBlank()) return v;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(argFlag)) return args[i + 1];
        }
        return defaultValue;
    }
}
