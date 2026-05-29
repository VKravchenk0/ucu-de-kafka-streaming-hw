FROM python:3.12-slim
WORKDIR /app
COPY web/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY web/main.py .
COPY web/templates ./templates
CMD ["python", "main.py"]
