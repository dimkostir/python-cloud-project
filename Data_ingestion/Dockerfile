FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_ingestion.py .

CMD ["python", "data_ingestion.py"]
