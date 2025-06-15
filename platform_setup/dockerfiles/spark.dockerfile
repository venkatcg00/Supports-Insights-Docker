FROM apache/spark:3.5.6

USER root

ENV PATH="/opt/spark/bin:$PATH"

# Install build dependencies
RUN apt-get update && apt-get install -y \
    wget build-essential \
    zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev \
    libssl-dev libreadline-dev libffi-dev libsqlite3-dev \
    libbz2-dev liblzma-dev uuid-dev \
    curl ca-certificates \
    && apt-get clean

# Build and install Python 3.11.13 from source
RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/3.11.13/Python-3.11.13.tgz && \
    tar xzf Python-3.11.13.tgz && \
    cd Python-3.11.13 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/local/bin/pip3.11 /usr/bin/pip3 && \
    python3 --version

# Install PySpark (must match Spark version)
RUN pip3 install --no-cache-dir pyspark==3.5.6

# Configure Spark to use Python 3.11
ENV PYSPARK_PYTHON="/usr/bin/python3"
ENV PYSPARK_DRIVER_PYTHON="/usr/bin/python3"

USER spark
