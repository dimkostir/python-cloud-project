FROM python:3.9-slim

WORKDIR /app

COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor/processor.py ./processor.py

COPY Data_ingestion ./Data_ingestion

CMD ["python", "processor.py"]
