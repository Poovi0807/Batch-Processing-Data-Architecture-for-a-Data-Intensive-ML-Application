FROM python:3.12-slim

# Run as a normal user, never as root
RUN useradd --create-home --uid 1001 app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .
USER app

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Uploads of big files can take minutes, hence the long timeout
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "1800", "ingest:app"]
