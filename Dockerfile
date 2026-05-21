FROM python:3.10-slim-bookworm

RUN apt-get update && \
    apt install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PYTHONPATH=/app/src
ENV APP_ENV=prod

COPY src/common ./src/common
COPY src/stream_processor ./src/stream_processor

CMD ["python", "-m", "stream_processor.main"]