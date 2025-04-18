#!/bin/bash

# Exit immediately if any command fails
set -e

# Print commands before executing
set -x

echo "Starting build and run process..."

# Store the original directory
ORIGINAL_DIR=$(pwd)

# Step 1: Change to rate-limiter directory
echo "Changing to rate-limiter directory..."
cd rate-limiter
if [ $? -ne 0 ]; then
  echo "Failed to change to rate-limiter directory"
  exit 1
fi

# Step 2: Build Docker image
echo "Building Docker image..."
docker build -t custom-rate-limiter:latest .
if [ $? -ne 0 ]; then
  echo "Docker build failed"
  cd "$ORIGINAL_DIR"
  exit 1
fi

# Step 3: Return to parent directory
echo "Returning to parent directory..."
cd "$ORIGINAL_DIR"
if [ $? -ne 0 ]; then
  echo "Failed to return to parent directory"
  exit 1
fi

# Step 4: Run docker-compose with custom rate limiter image
echo "Running docker-compose with custom rate limiter image..."
RATE_LIMITER_IMAGE=custom-rate-limiter:latest docker-compose up
if [ $? -ne 0 ]; then
  echo "Docker compose failed"
  exit 1
fi

echo "Process completed successfully!"

