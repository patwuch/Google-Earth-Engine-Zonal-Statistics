FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    libproj-dev \
    gdal-bin \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install backend deps first (layer-caching)
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Copy full repo (Snakefile, scripts, etc. are needed at runtime)
COPY . /app

EXPOSE 8000

CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "300"]
