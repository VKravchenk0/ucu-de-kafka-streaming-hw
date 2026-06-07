.PHONY: infra-up infra-down \
        build-cpu build-gpu \
        up-cpu up-gpu \
        down logs \
        test

# ----------------------------------------------------------------------------
# Infrastructure (Kafka, ksqlDB, Control Center, …)
# ----------------------------------------------------------------------------

infra-up:
	docker compose --profile infra up -d

infra-down:
	docker compose --profile infra down

# ----------------------------------------------------------------------------
# Build application images
# build-cpu  → capstone-car-detector:cpu   capstone-person-detector:cpu   (~200 MB torch)
# build-gpu  → capstone-car-detector:cuda  capstone-person-detector:cuda  (~2 GB CUDA torch)
#
# Both tags can coexist in the local image store; up-cpu / up-gpu select the right one.
# ----------------------------------------------------------------------------

build-cpu:
	docker compose --profile app build

build-gpu:
	PROCESSING_UNIT_TYPE=cuda docker compose \
	  -f docker-compose.yaml -f docker-compose.gpu.yaml \
	  --profile app build

# ----------------------------------------------------------------------------
# Run application stack
# ----------------------------------------------------------------------------

up-cpu:
	docker compose --profile app up -d

up-gpu:
	PROCESSING_UNIT_TYPE=cuda docker compose \
	  -f docker-compose.yaml -f docker-compose.gpu.yaml \
	  --profile app up -d

down:
	docker compose --profile app down

logs:
	docker compose --profile app logs -f

# ----------------------------------------------------------------------------
# E2E tests — starts infra+app, runs tests, tears down (set E2E_KEEP_STACK=1
# to leave the stack running after the test for inspection)
# ----------------------------------------------------------------------------

test:
	python3 -m venv tests/.venv
	tests/.venv/bin/pip install -q -r tests/requirements.txt
	tests/.venv/bin/pytest tests/ -v
