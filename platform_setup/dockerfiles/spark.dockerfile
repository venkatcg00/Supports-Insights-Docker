FROM bitnami/spark:3.4.1

USER root

# Install curl and clean up
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install dependencies for building Python
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libbz2-dev \
    wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download and install Python 3.12.10 from Python archive
RUN wget https://www.python.org/ftp/python/3.12.10/Python-3.12.10.tgz && \
    tar -xzf Python-3.12.10.tgz && \
    cd Python-3.12.10 && \
    ./configure --enable-optimizations --prefix=/usr/local && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.12.10 Python-3.12.10.tgz

# Remove Bitnami's Python binaries to avoid conflicts
RUN rm -f /opt/bitnami/python/bin/python* /opt/bitnami/python/bin/pip* /usr/bin/python3 /usr/bin/python

# Create symbolic links to make Python 3.12.10 the default python3
RUN ln -sf /usr/local/bin/python3.12 /usr/bin/python3 && \
    ln -sf /usr/local/bin/python3.12 /usr/bin/python

# Update PATH to prioritize /usr/local/bin
ENV PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# Verify Python version and binary details
RUN python3 --version && \
    which python3 && \
    ls -l /usr/bin/python3 && \
    /usr/local/bin/python3.12 --version && \
    echo $PATH

USER 1001