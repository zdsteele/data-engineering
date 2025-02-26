# Dockerized Streaming Comparison: Batch vs. Realtime Streaming with Airline Dataset

## Overview

This project sets up a Docker-based environment to compare and contrast batch streaming versus real-time streaming using an airline dataset. The environment consists of multiple services including Kafka, Zookeeper, Jupyter, Spark, and a Flask application.

## Services

### 1. **Zookeeper**

- **Image:** `confluentinc/cp-zookeeper`
- **Purpose:** Manages Kafka brokers
- **Port:** `2181`

### 2. **Kafka**

- **Image:** `bitnami/kafka:3.5.1`
- **Purpose:** Message broker for streaming data
- **Ports:** `9092:9092`
- **Environment Variables:**
  - `KAFKA_CFG_LISTENERS`: Configures Kafka listeners
  - `KAFKA_CFG_ZOOKEEPER_CONNECT`: Connects Kafka to Zookeeper
  - `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE`: Enables auto-creation of topics
- **Depends on:** Zookeeper

### 3. **Jupyter**

- **Builds from:** `./jupyter/Dockerfile.jupyter`
- **Purpose:** Interactive data analysis and visualization
- **Ports:** `8888:8888`
- **Volumes:**
  - `./data:/home/jovyan/data`
  - `./jupyter/notebooks:/home/jovyan/notebooks`

### 4. **Spark**

- **Builds from:** `./spark/Dockerfile`
- **Purpose:** Batch and real-time data processing
- **Depends on:** Kafka, Flask App
- **Volumes:**
  - `./data:/data`

### 5. **Flask App**

- **Builds from:** `./flask_app`
- **Purpose:** Web service for interacting with the streaming system
- **Ports:** `5000:5000`
- **Depends on:** Kafka

## Setup Instructions

### Prerequisites

- Docker & Docker Compose installed

### Running the Project

1. Clone the repository
   ```sh
   git clone <repo-url>
   cd <repo-directory>
   ```
2. Start the Docker containers
   ```sh
   docker-compose up --build
   ```
3. Access services:
   - Jupyter Notebook: [http://127.0.0.1:8888/lab](http://127.0.0.1:8888/lab)
   - Flask API: [http://127.0.0.1:5000](http://127.0.0.1:5000)

### Stopping the Containers

```sh
docker-compose down
```

## Data Flow

1. The airline dataset is either streamed in real-time or processed in batch mode.
2. Kafka brokers the data streams.
3. Spark processes data in either batch or streaming mode.
4. Jupyter Notebooks allow for visualization and analysis.
5. Flask provides an API for interaction with the processed data.

## Contributions

Feel free to open issues or pull requests to enhance the project.

