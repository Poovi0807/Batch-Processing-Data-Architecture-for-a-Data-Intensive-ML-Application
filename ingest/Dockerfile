# Ingestion service: small Python web service + the data preparation script
FROM python:3.12-slim

RUN useradd --create-home --uid 1000 ingest
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY prepare_data.py ingest_service.py ./

USER ingest
CMD ["python", "ingest_service.py"]
