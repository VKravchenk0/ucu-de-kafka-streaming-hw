FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Select the torch distribution at build time.
#   PROCESSING_UNIT_TYPE=cpu  → CPU-only wheels  (~200 MB, default)
#   PROCESSING_UNIT_TYPE=cuda → CUDA 12.1 wheels (~2 GB, requires NVIDIA runtime at run time)
# torch/torchvision must NOT appear in requirements.txt — pip would resolve the GPU build without
# the index URL and pull ~1.5 GB of extra CUDA libraries even on CPU-only hosts.
ARG PROCESSING_UNIT_TYPE=cpu
RUN if [ "$PROCESSING_UNIT_TYPE" = "cuda" ]; then \
      pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cu121; \
    else \
      pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu; \
    fi

COPY detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the YOLOv8n weights at build time so containers start fast
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

COPY common ./common
COPY detector/main.py .

CMD ["python", "main.py"]
