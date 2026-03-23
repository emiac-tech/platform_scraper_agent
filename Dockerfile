# Setup the Python runtime
FROM python:3.10-slim

# Set environment paths
WORKDIR /app

# Install system dependencies (needed for compiling some python packages like lxml, pandas)
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy strictly the requirements file first to cache the layer
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application ecosystem
COPY . /app/

# Create necessary mount points for Docker Volumes
# - /app/data: Holds the .env file and the SQLite marketplaces.db
# - /app/logs: Centralizes the system.log
VOLUME ["/app/data", "/app/logs"]

# Expose the Webhook port
EXPOSE 8000

# Default command falls back to running the orchestrator defensively, 
# although docker-compose strongly dictates the precise execution vectors.
CMD ["python3", "orchestrator.py", "--daemon"]
