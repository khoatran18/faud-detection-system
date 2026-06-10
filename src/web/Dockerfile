# ==========================================
# Stage 1: Build the React frontend
# ==========================================
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

# Copy dependency manifests
COPY src/web/frontend/package.json src/web/frontend/package-lock.json ./
RUN npm ci

# Copy source code and build
COPY src/web/frontend/ ./
RUN npm run build

# ==========================================
# Stage 2: Final production image
# ==========================================
FROM python:3.10-slim-bookworm

# Install basic diagnostic tools
RUN apt-get update && \
    apt install -y --no-install-recommends \
    curl \
    procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app/src
ENV APP_ENV=prod

# Copy shared configs and source code
COPY src/common ./src/common
COPY src/config ./src/config
COPY src/web/backend ./src/web/backend

# Copy the built frontend static assets from Stage 1
COPY --from=frontend-builder /app/frontend/dist ./src/web/frontend/dist

# Expose FastAPI port
EXPOSE 8000

# Set working directory to backend to run uvicorn correctly
WORKDIR /app/src/web/backend

# Run uvicorn directly for production performance (without auto-reload)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

