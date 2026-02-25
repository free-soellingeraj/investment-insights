FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose Cloud Run port
EXPOSE 8080

# Run Litestar directly — single process, no supervisord
CMD ["litestar", "run", "--app", "web.app:app", "--host", "0.0.0.0", "--port", "8080"]
