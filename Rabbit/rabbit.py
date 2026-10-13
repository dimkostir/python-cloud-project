import os
import json
import time
import pika
import requests

RABBIT_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBIT_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBIT_USER = os.environ.get("RABBITMQ_USER", "admin")
RABBIT_PASS = os.environ.get("RABBITMQ_PASS", "admin123")

OBS_QUEUE = os.environ.get("OBS_QUEUE", "weather.observations")
ALERT_QUEUE = os.environ.get("ALERT_QUEUE", "alerts.temp_deviation")

PREDICT_URL = os.environ.get("PREDICT_URL", "http://flask-api:5000/predict")


def connect_with_retry(max_tries=30, sleep_seconds=2):
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=30,
    )

    for i in range(max_tries):
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            time.sleep(sleep_seconds)

    raise RuntimeError("Could not connect to RabbitMQ after retries")


def main():
    connection = connect_with_retry()
    channel = connection.channel()

    channel.queue_declare(queue=OBS_QUEUE, durable=True)
    channel.queue_declare(queue=ALERT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        try:
            msg = json.loads(body.decode("utf-8"))
        except Exception:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        features = msg.get("features")
        if not isinstance(features, dict):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Call API /predict
        try:
            resp = requests.post(PREDICT_URL, json={"features": features}, timeout=15)
        except Exception:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        if resp.status_code != 200:
            if 500 <= resp.status_code <= 599:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        result = resp.json()
        predicted = float(result["predicted_temperature_2m"])
        threshold = float(result["threshold"])

        actual = msg.get("actual_temperature_2m")

        if actual is not None:
            error = abs(float(actual) - predicted)

            if error > threshold:
                alert = {
                    "status": "ALARM",
                    "predicted_temperature_2m": predicted,
                    "actual_temperature_2m": float(actual),
                    "error": error,
                    "threshold": threshold,
                    "rule": result.get("rule", "2x_rmse"),
                    "timestamp": msg.get("timestamp"),
                }

                channel.basic_publish(
                    exchange="",
                    routing_key=ALERT_QUEUE,
                    body=json.dumps(alert).encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2),
                )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=OBS_QUEUE, on_message_callback=on_message)
    print(f"Consumer listening on queue: {OBS_QUEUE}")
    channel.start_consuming()


if __name__ == "__main__":
    main()
