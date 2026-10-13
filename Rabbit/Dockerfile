FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir pika requests

COPY rabbit.py .

CMD ["python", "rabbit.py"]
