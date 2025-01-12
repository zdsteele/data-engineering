FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY kafka_producer.py .

# Default command
#CMD ["python", "kafka_producer.py"]
CMD ["bash"]
