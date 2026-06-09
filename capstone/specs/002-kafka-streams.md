# Spec: e2e test
## Goal
Tracking and counters works on kafka streams

## Component / Module
The part of the pipeline between `detection.*` and `tracking.combined` topics should be implemented directly on kafka streams instead of custom processor + ksqldb

## Input / Props / Request
Data from `detections.cars` and `detections.persons` topics

## Output / Behavior
- Business behavior doesn't change
- `detection.cars` and `detection.persons` are the sources for the topology
- `tracking.combined` is the sink of the topology
- The total counts of the detected objects are calculated with the means of kafka streams
- ksqldb is no longer used

## Non-goals
Don't update the tests unless they contain the error by themselves

## Acceptance Criteria
- Existing test are passing