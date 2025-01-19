FROM python:3.9-slim

WORKDIR /app

# Copy requirements.txt into the container
COPY requirements.txt requirements.txt

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy the Flask app and all related files
COPY . .

CMD ["python", "app.py"]
