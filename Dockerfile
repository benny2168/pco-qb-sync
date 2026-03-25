FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (for Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY sync_pc_to_qb.py .
COPY sync_donations_qb_to_pc.py .
COPY templates templates
COPY static static

# Create necessary directories
RUN mkdir -p /app/config /app/data /app/logs

EXPOSE 8080

CMD ["python", "app.py"]
