import json
import yaml
import logging
import time
from typing import List

from confluent_kafka import Producer

class KafkaDataSender:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Allow either bootstrap.servers (confluent style) or bootstrap_servers (legacy in this repo)
        self.bootstrap_servers = (
            config.get('bootstrap.servers')
            or config.get('bootstrap_servers')
            or 'localhost:9092'
        )
        self.topic_prefix = config.get('default_topic_prefix', 'etl')
        self.batch_size = int(config.get('batch_size', 100))
        self.retry_max = int(config.get('retry_max', 3))
        self.throttle_time = float(config.get('throttle_time', 0.05))
        self.topic_map = config.get('topics', {}) or {}

        producer_conf = {
            'bootstrap.servers': self.bootstrap_servers,
            # Safer defaults
            'enable.idempotence': True,
            # Optional tunables with sensible fallbacks
            'compression.type': config.get('compression.type', 'snappy'),
            'linger.ms': int(config.get('linger.ms', 50)),
            'batch.num.messages': int(config.get('batch.num.messages', 10000)),
        }

        self.producer = Producer(producer_conf)

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('KafkaDataSender')

    # Delivery callback for logging
    def _delivery_report(self, err, msg):
        if err is not None:
            self.logger.warning(f"Delivery failed for topic {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}: {err}")
        else:
            # Uncomment for verbose success logs
            # self.logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]@{msg.offset()}")
            pass

    def _resolve_topic(self, topic_suffix: str) -> str:
        # If a full topic is provided via config map, use it; else build from prefix
        return self.topic_map.get(topic_suffix, f"{self.topic_prefix}.{topic_suffix}")

    def _produce_with_retry(self, topic: str, value_bytes: bytes) -> bool:
        for attempt in range(1, self.retry_max + 1):
            try:
                self.producer.produce(topic, value=value_bytes, on_delivery=self._delivery_report)
                # Serve delivery callbacks; 0 means non-blocking
                self.producer.poll(0)
                return True
            except BufferError as e:
                # Local queue is full; give librdkafka a chance to send
                self.logger.warning(f"[Retry {attempt}] Local queue full while sending to {topic}: {e}")
                # Poll for a bit to drain callbacks and clear queue
                self.producer.poll(0.5)
                time.sleep(min(0.2 * attempt, 1.0))
            except Exception as e:
                self.logger.warning(f"[Retry {attempt}] Error producing to {topic}: {e}")
                time.sleep(min(0.2 * attempt, 1.0))
        self.logger.error(f"Fail to send record to {topic} after {self.retry_max} attempts")
        return False

    def send_dataframe(self, df, topic_suffix: str):
        topic = self._resolve_topic(topic_suffix)
        records = df.to_dict(orient='records')
        self._send_batch(records, topic)

    def send_documents(self, documents: List[dict], topic_suffix: str):
        topic = self._resolve_topic(topic_suffix)
        self._send_batch(documents, topic)

    def _send_batch(self, records: list, topic: str):
        self.logger.info(f"sending {len(records)} records to topic {topic}")
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            for record in batch:
                payload = json.dumps(record, default=str).encode('utf-8')
                self._produce_with_retry(topic, payload)
            self.logger.info(f"Sent batch {i // self.batch_size + 1} to {topic}")
            # Give the producer a moment and also honor any throttling desired
            self.producer.poll(0)
            time.sleep(self.throttle_time)

    def close(self):
        # Block until all messages are delivered or timeout expires
        try:
            self.producer.flush(10.0)
        except Exception:
            # Ensure we always attempt to drain
            self.producer.flush()
