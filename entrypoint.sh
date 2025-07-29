#!/bin/sh
set -e

# The code will live directly in the WORKDIR, which is /app
echo "Checking Git status in `pwd`..."

# Check if the repository is already cloned
if [ -d ".git" ]; then
  echo "Git repository found. Pulling latest changes..."
  git pull
else
  echo "Cloning repository from $GIT_REPO_URL..."
  # Clone the repository into the current directory
  git clone "$GIT_REPO_URL" .
fi

echo "✅ Code is up to date."

# Execute the command passed to the container
exec "$@"