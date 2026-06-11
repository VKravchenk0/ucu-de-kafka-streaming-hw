package com.capstone.tracking;

import com.capstone.tracking.model.CombinedRecord;
import com.capstone.tracking.model.DetectionRecord;
import com.capstone.tracking.model.TrackerState;
import com.capstone.tracking.model.TrackingRecord;
import com.capstone.tracking.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Single Kafka Streams app implementing the rekey/aggregate/join topology that
 * replaces the standalone Python car-tracker / person-tracker services:
 *
 * <pre>
 * detections.cars    --[track: CentroidTracker + cumulative "seen" set]--&gt; rekey by session_frame --\
 *                                                                                                      LEFT JOIN (2s window + 500ms grace) --&gt; rekey by session --&gt; tracking.combined
 * detections.persons --[track: CentroidTracker + cumulative "seen" set]--&gt; rekey by session_frame --/
 * </pre>
 */
public class Main {
    private static final int MAX_DISAPPEARED = 30;
    private static final double MAX_DISTANCE = 200.0;
    private static final double MIN_IOU = 0.1;

    public static void main(String[] args) {
        String bootstrapServers = require("KAFKA_BOOTSTRAP_SERVERS");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tracking-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put("producer.linger.ms", "5");

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<DetectionRecord> detectionSerde = new JsonSerde<>(DetectionRecord.class);
        JsonSerde<TrackingRecord> trackingSerde = new JsonSerde<>(TrackingRecord.class);
        JsonSerde<CombinedRecord> combinedSerde = new JsonSerde<>(CombinedRecord.class);
        JsonSerde<TrackerState> trackerStateSerde = new JsonSerde<>(TrackerState.class);

        KStream<String, TrackingRecord> carsByFrame = trackedStream(
                builder, "detections.cars", "car", "car-tracker-state",
                detectionSerde, trackingSerde, trackerStateSerde);
        KStream<String, TrackingRecord> personsByFrame = trackedStream(
                builder, "detections.persons", "person", "person-tracker-state",
                detectionSerde, trackingSerde, trackerStateSerde);

        // LEFT JOIN anchored on cars: detector/main.py emits one message per preprocessed
        // frame on *both* detection topics (even with an empty detections list), so every
        // join key is guaranteed to exist on both sides eventually — nothing is lost. A
        // FULL OUTER join would instead risk emitting spurious duplicate records under
        // transient producer skew (a persons-only record, then a second "both sides" record
        // once cars catches up within the grace period).
        KStream<String, CombinedRecord> combined = carsByFrame.leftJoin(
                personsByFrame,
                CombinedRecord::of,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(2), Duration.ofMillis(500)),
                StreamJoined.with(Serdes.String(), trackingSerde, trackingSerde)
        );

        combined
                .selectKey((frameKey, rec) -> rec.session_id)
                .to("tracking.combined", Produced.with(Serdes.String(), combinedSerde));

        var topology = builder.build();

        // Retry KafkaStreams construction until the broker is reachable. The admin client
        // inside the constructor fails immediately with a DNS resolution error when the
        // broker container hasn't started yet — unlike the Python confluent-kafka library
        // which retries internally. Docker's restart policy would also handle this, but
        // in-process retry avoids filling logs with repeated crash+restart cycles.
        KafkaStreams streams = null;
        for (int attempt = 1; ; attempt++) {
            try {
                streams = new KafkaStreams(topology, props);
                break;
            } catch (Exception e) {
                long delaySecs = Math.min(30, attempt * 2L);
                System.err.printf("Broker not ready (attempt %d), retrying in %ds: %s%n",
                        attempt, delaySecs, e.getMessage());
                try {
                    TimeUnit.SECONDS.sleep(delaySecs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    /**
     * Source -&gt; stateful centroid tracking (cumulative unique count) -&gt; rekey by
     * {@code session_id + "_" + frame_number} so the two object-type streams can be
     * exact-matched per frame in the windowed join.
     */
    private static KStream<String, TrackingRecord> trackedStream(
            StreamsBuilder builder,
            String inputTopic,
            String objectType,
            String storeName,
            JsonSerde<DetectionRecord> detectionSerde,
            JsonSerde<TrackingRecord> trackingSerde,
            JsonSerde<TrackerState> trackerStateSerde) {

        StoreBuilder<KeyValueStore<String, TrackerState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName), Serdes.String(), trackerStateSerde);
        builder.addStateStore(storeBuilder);

        ProcessorSupplier<String, DetectionRecord, String, TrackingRecord> processorSupplier =
                () -> new TrackingProcessor(objectType, storeName, MAX_DISAPPEARED, MAX_DISTANCE, MIN_IOU);

        return builder
                .stream(inputTopic, Consumed.with(Serdes.String(), detectionSerde))
                .process(processorSupplier, storeName)
                .selectKey((sessionId, rec) -> sessionId + "_" + rec.frame_number);
    }

    private static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }
}
