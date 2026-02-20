FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Install OpenJDK 11 for PySpark 3.5.x compatibility.
# default-jdk-headless on Debian Bullseye provides OpenJDK 11.
# /usr/lib/jvm/default-java is an architecture-independent symlink.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Install uv system-wide so it is available to all users without PATH manipulation.
RUN pip install --no-cache-dir uv
