# Use Bookworm base to avoid missing packages
FROM python:3.10-slim-bookworm

# Install OpenJDK 17 and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ca-certificates curl gnupg lsb-release && \
    apt-get clean && \
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile && \
    echo "export PATH=$JAVA_HOME/bin:$PATH" >> /etc/profile && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "PATH=$JAVA_HOME/bin:$PATH" >> /etc/environment

# Manually set default fallback (only used by Python apps during build)
# This avoids needing shell substitution inside ENV
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