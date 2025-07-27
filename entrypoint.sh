#!/bin/sh
set -e

# The directory where your code will live
CODE_DIR="/usr/src/app/code"

# Create the directory if it doesn't exist
mkdir -p $CODE_DIR
cd $CODE_DIR

# Check if the repository is already cloned
if [ -d ".git" ]; then
  echo "Git repository found. Pulling latest changes..."
  git pull
else
  echo "Cloning repository from $GIT_REPO_URL..."
  # Clone the repository and handle potential empty directory issues
  git clone "$GIT_REPO_URL" .
fi

echo "✅ Code is up to date."

# Execute the command passed to the container (the CMD from the Dockerfile)
exec "$@"