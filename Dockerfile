# Use Bookworm base to avoid missing packages
FROM python:3.10-slim-bookworm

# Install Java and essential build tools
RUN apt-get update && \
    apt-get install -y \
    openjdk-17-jdk \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    gcc \
    python3-dev \
    && apt-get clean

# Set JAVA_HOME and PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Python-specific ENV
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory and copy code
WORKDIR /app
COPY . /app/

# Install Python packages
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Default command
CMD ["python", "main.py"]