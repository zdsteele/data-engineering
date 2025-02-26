FROM bitnami/spark:latest

USER root

# Copy requirements.txt into the container
COPY app/requirements.txt /opt/spark/app/requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install -r /opt/spark/app/requirements.txt

# Copy the Spark app into the container
COPY app/spark_app.py /opt/spark/app/spark_app.py

CMD ["/bin/bash", "-c", "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /opt/spark/app/spark_app.py"]
