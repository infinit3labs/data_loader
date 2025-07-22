# Databricks Data Loader - Docker Testing Environment
FROM openjdk:11-jre-slim

# Set environment variables
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV DELTA_VERSION=2.4.0
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python/:${PYTHONPATH}"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create spark user
RUN useradd -m -s /bin/bash spark && \
    mkdir -p /opt/spark && \
    chown -R spark:spark /opt/spark

# Install Spark
RUN cd /opt && \
    wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"/* spark/ && \
    rm -rf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Delta Lake jars
RUN cd /opt/spark/jars && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# Set up working directory
WORKDIR /app

# Copy Python requirements first for better caching
COPY pyproject.toml poetry.lock* ./

# Install Poetry and Python dependencies
RUN pip3 install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Copy application code
COPY . .

# Create necessary directories for data and logs
RUN mkdir -p \
    /app/data/raw \
    /app/data/processed \
    /app/data/checkpoints \
    /app/logs \
    /app/test-results \
    && chown -R spark:spark /app

# Switch to spark user
USER spark

# Set up Spark configuration for Delta Lake
RUN mkdir -p /opt/spark/conf && \
    echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" > /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf

# Expose ports for Spark UI and debugging
EXPOSE 4040 4041 8080 8081

# Default command
CMD ["python3", "-m", "data_loader.main", "--help"]
