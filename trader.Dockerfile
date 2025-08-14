# Start from a Python base image
FROM python:3.11-slim

# Install system dependencies
# - git: for version control if needed
# - fontconfig, fonts-dejavu-core: for Plotly chart generation
# - procps: provides utilities like 'ps', 'pkill', and 'top' for process management
RUN apt-get update && apt-get install -y git fontconfig fonts-dejavu-core procps && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The command to keep the container running
CMD ["tail", "-f", "/dev/null"]