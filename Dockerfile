# Use modern Python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies if any (none needed currently)
# RUN apt-get update && apt-get install -y --no-install-recommends ... && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (for layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else (using .dockerignore for exclusions)
COPY . .

# Ensure necessary directories exist for volume mounting
RUN mkdir -p /app/config /app/data /app/logs

EXPOSE 7365

# Use a slightly more robust startup command if needed
CMD ["python", "app.py"]
