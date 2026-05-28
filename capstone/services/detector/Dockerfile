FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY detector/requirements.txt .
# Install CPU-only torch + torchvision to avoid pulling CUDA libraries (~3 GB)
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download YOLOv8n weights so they are baked into the image
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"
COPY common ./common
COPY detector/main.py .
CMD ["python", "main.py"]
