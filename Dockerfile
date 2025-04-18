FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y gcc build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Configure Poetry to not create virtual environment in container
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-root

# Create logs directory
RUN mkdir logs

# Copy application code
COPY . .

# Make entrypoint script executable
RUN chmod +x scripts/entrypoint.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Use entrypoint script to configure and start the service
CMD ["python", "scripts/entrypoint.py"]
