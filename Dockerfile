FROM python:3.10-slim

# 1. Install system C-libraries for Geospatial (GDAL) AND Database (PostGIS)
RUN apt-get update && apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 2. Set environment variables so Python finds the C-headers
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app

# 3. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 4. Copy application code
COPY . .

# 5. Dagster Configuration for Cloud Run Job
ENV DAGSTER_APP=orchestration
CMD ["dagster", "asset", "materialize", "--select", "*"]