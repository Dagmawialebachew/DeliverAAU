# Use Python 3.12 slim base image for latest performance improvements
FROM python:3.12-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies needed for psycopg/asyncpg
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for caching)
COPY requirements.txt .

# Upgrade pip/setuptools/wheel and install dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose port (Render sets $PORT automatically)
EXPOSE 8080

# Run the bot directly — aiohttp server is started inside bot.py
CMD ["python", "bot.py"]
