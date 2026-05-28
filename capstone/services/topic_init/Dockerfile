FROM python:3.12-slim
WORKDIR /app
COPY topic_init/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY topic_init/main.py .
CMD ["python", "main.py"]
