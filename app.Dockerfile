# Start from a Python base image
FROM python:3.11-slim

# Install git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Set a simpler working directory
WORKDIR /app

# Copy all project files (requirements, entrypoint, etc.) into the container
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]

# The default command to run after the entrypoint
CMD ["tail", "-f", "/dev/null"]