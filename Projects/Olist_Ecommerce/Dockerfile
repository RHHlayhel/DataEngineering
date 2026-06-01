FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY scripts/ ./scripts/
COPY data/    ./data/

# Credentials are passed at runtime via --env-file, never baked in
CMD ["python", "scripts/sql_feeding.py"]
