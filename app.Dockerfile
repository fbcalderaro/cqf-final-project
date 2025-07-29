# Start from a Python base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The command to keep the container running
CMD ["tail", "-f", "/dev/null"]