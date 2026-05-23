import os
import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Row
from typing import Iterable

class TradeTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[3])

class OHLCWindowFunction(ProcessWindowFunction):
    def process(self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[tuple]) -> Iterable[Row]:
        trades = list(elements)
        
        prices = [trade[1] for trade in trades]
        volumes = [trade[2] for trade in trades]
        
        open_idx = trades.index(min(trades, key=lambda t: t[3]))
        close_idx = trades.index(max(trades, key=lambda t: t[3]))
        
        open_price = float(trades[open_idx][1])
        close_price = float(trades[close_idx][1])
        high_price = float(max(prices))
        low_price = float(min(prices))
        total_volume = float(sum(volumes))
        
        window_start = context.window().start
        window_time = datetime.utcfromtimestamp(window_start / 1000.0)
        
        yield Row(window_time, key, open_price, high_price, low_price, close_price, total_volume)

def extract_trade_data(message_str):
    try:
        data = json.loads(message_str)
        return (data['s'], float(data['p']), float(data['v']), int(data['t']))
    except Exception as e:
        return None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ADD KAFKA AND JDBC CONNECTOR JARS
    kafka_jar = "file:///opt/flink/streaming/jars/flink-sql-connector-kafka-1.17.2.jar"
    jdbc_jar = "file:///opt/flink/streaming/jars/flink-connector-jdbc-3.1.1-1.17.jar"
    pg_jar = "file:///opt/flink/streaming/jars/postgresql-42.6.0.jar"
    env.add_jars(kafka_jar, jdbc_jar, pg_jar)

    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    topic = "trades"

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_broker) \
        .set_topics(topic) \
        .set_group_id("flink_ohlc_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    row_type_info = Types.ROW([
        Types.SQL_TIMESTAMP(), 
        Types.STRING(), 
        Types.DOUBLE(), 
        Types.DOUBLE(), 
        Types.DOUBLE(), 
        Types.DOUBLE(), 
        Types.DOUBLE()
    ])

    processed_stream = stream \
        .map(extract_trade_data, output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.LONG()])) \
        .filter(lambda x: x is not None) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(TradeTimestampAssigner())
        ) \
        .key_by(lambda event: event[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(OHLCWindowFunction(), output_type=row_type_info)

    # TimescaleDB JDBC Sink — upsert to avoid duplicates on job restart
    jdbc_sink = JdbcSink.sink(
        """INSERT INTO candles (time, symbol, open, high, low, close, volume)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (time, symbol) DO UPDATE
           SET open   = EXCLUDED.open,
               high   = EXCLUDED.high,
               low    = EXCLUDED.low,
               close  = EXCLUDED.close,
               volume = EXCLUDED.volume""",
        row_type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_url('jdbc:postgresql://timescaledb:5432/market_data') \
            .with_driver_name('org.postgresql.Driver') \
            .with_user_name('postgres') \
            .with_password('postgres') \
            .build(),
        JdbcExecutionOptions.builder() \
            .with_batch_interval_ms(1000) \
            .with_batch_size(200) \
            .with_max_retries(5) \
            .build()
    )

    processed_stream.add_sink(jdbc_sink)
    
    env.execute("PyFlink OHLC Processing to TimescaleDB")

if __name__ == '__main__':
    main()
