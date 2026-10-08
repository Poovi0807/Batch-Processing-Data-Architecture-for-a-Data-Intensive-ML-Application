FROM python:3.12-slim

# run as an unprivileged user
RUN useradd --create-home --uid 1000 ingest
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/
USER ingest

EXPOSE 8000
CMD ["python", "server.py"]
