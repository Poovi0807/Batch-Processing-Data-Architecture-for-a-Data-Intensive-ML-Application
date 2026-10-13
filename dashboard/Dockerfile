# Pipeline dashboard (read-only web page)
FROM python:3.12-slim

RUN useradd --create-home --uid 1000 dashboard
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

USER dashboard
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false", "--client.toolbarMode=minimal"]
