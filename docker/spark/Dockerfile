FROM apache/spark:3.5.1-java17

USER root

# Install Python and required utilities
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages for Iceberg and AWS
RUN pip install --no-cache-dir \
    pyiceberg==0.7.0 \
    boto3 \
    requests \
    pyspark

USER spark

WORKDIR /home/spark

# Set environment variables for Spark and Python
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

EXPOSE 8080 10000 10001

# Default command
CMD ["tail", "-f", "/dev/null"]
