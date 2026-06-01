FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# CPU-only torch must be installed BEFORE ultralytics to avoid pulling the GPU build (~1.5 GB)
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the YOLOv8n weights at build time so containers start fast
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

COPY common ./common
COPY detector/main.py .

CMD ["python", "main.py"]
