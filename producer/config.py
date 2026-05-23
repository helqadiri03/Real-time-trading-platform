import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = "trades"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
