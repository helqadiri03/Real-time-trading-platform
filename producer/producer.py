import json
import logging
import websocket
import time
from confluent_kafka import Producer
from config import KAFKA_BROKER, TOPIC_NAME, FINNHUB_WS_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer_config = {'bootstrap.servers': KAFKA_BROKER}
kafka_producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get('type') == 'trade':
            for trade in data['data']:
                kafka_producer.produce(TOPIC_NAME, json.dumps(trade).encode('utf-8'), callback=delivery_report)
            kafka_producer.poll(0)
    except Exception as e:
        logger.error(f"Error: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket closed")
    kafka_producer.flush()

def on_open(ws):
    logger.info("WebSocket opened. Subscribing to multiple symbols...")
    symbols = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT"]
    for symbol in symbols:
        ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')

if __name__ == "__main__":
    websocket.enableTrace(False)
    while True:
        try:
            logger.info("Connecting to Finnhub WebSocket...")
            ws = websocket.WebSocketApp(FINNHUB_WS_URL, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.on_open = on_open
            ws.run_forever()
            logger.info("Connection dropped. Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Critical error: {e}")
            time.sleep(5)
