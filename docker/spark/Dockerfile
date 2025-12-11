FROM apache/spark:3.5.1-java17

USER root

# Install Python packages for dataset initialization
RUN pip install --no-cache-dir \
    boto3 \
    requests

USER spark

WORKDIR /home/spark

EXPOSE 8080 10000 10001

# Default command
CMD ["tail", "-f", "/dev/null"]
