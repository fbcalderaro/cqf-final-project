# Start from a Python base image
FROM python:3.11-slim

# Install system dependencies with the correct font package name
RUN apt-get update && apt-get install -y git fontconfig fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The command to keep the container running
CMD ["tail", "-f", "/dev/null"]