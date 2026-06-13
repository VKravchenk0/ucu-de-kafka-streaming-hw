# Spec: e2e test
## Goal
Kafka pipeline is covered with e2e tests

## Component / Module
Whole pipeline from uploading the file to reading the data from `tracking.combined` topic and from the websocket.

## Input / Props / Request
Input file to upload: `video-processing-pipeline/test-input.mp4`

## Output / Behavior
- Relevant records are present in `tracking.combined`
- Relevant records are consumed via the websocket

## Non-goals
Don't change the production code unless absolutely necessary. Only add tests.

## Acceptance Criteria
- Tests are present and are passing