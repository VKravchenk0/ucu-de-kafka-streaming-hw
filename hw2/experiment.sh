#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Load .env
set -a; source .env; set +a

echo "=== Experiment: P=${NUM_PARTITIONS} Prod=${NUM_PRODUCERS} Con=${NUM_CONSUMERS} ==="

# Reset topic so partition count takes effect
echo "--- Resetting topic '$TOPIC_NAME' ---"
docker exec broker kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic "$TOPIC_NAME" 2>/dev/null || true

# Clear previous run results
rm -f output/*.csv output/report.txt

# Start consumer
echo "--- Starting consumer ---"
docker compose up -d consumer

# Run producer (blocks until all frames are sent)
echo "--- Running producer ---"
docker compose run --rm producer

# Wait for consumer to drain and exit on its own (idle timeout)
echo "--- Waiting for consumer to finish ---"
docker compose wait consumer

# Generate report
echo "--- Generating report ---"
docker compose run --rm stat-aggregator

# Archive report with experiment parameters
REPORT_NAME="output/report-$(date +%s)-P${NUM_PARTITIONS}-Prod${NUM_PRODUCERS}-Con${NUM_CONSUMERS}.txt"
cp output/report.txt "$REPORT_NAME"
echo "--- Report saved to $REPORT_NAME ---"
