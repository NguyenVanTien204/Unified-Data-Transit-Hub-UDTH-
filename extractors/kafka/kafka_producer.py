from kafka import KafkaProducer
import json
import yaml
import logging
import time
import kafka.errors as KafkaError
import pandas as pd

class KafkaDataSender:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.bootstrap_server = config['bootstrap_servers']
        self.topic_prefix = config.get('default_topic_prefix', 'etl')
        self.batch_size = config.get('batch_size',100)    
        self.retry_max = config.get('retry_max', 3)
        self.throttle_time = config.get('throttle_time', 0.05)
        self.topic_map = config.get('topics', {})

        self.producer = KafkaProducer(
            bootstrap_server= self.bootstrap_server, value_serializer=lambda v: json.dump(v, default= str).encode('utf-8')
        )

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("KafkaDataSender")


    def _send_data_with_retry(self, topic, record):
        for attempt in range(1, self.retry_max + 1):
            try:
                self.producer.send(topic, record)
                return True
            except KafkaError as e:
                self.logger.warning(f"[Retry {attempt}] Error sending to {topic}: {e}")
                time.sleep(0.5)
        self.logger.error(f"Fail to send record to {topic} after {self.retry_max} attempts")
        return False    

    def send_dataframe(self, df,topic_suffix: str):
        topic = f"{self.topic_prefix}.{topic_suffix}"
        records = df.to_dict(orient = "records")
        self._send_batch(records, topic)

    def send_documents(self, documents: list, topic_suffix: str):
        topic = f"{self.topic_prefix}.{topic_suffix}"
        self._send_batch(documents, topic) 

    def _send_batch(self, records: list, topic: str):
        self.logger.info(f"sending {len(records)} records to topic {topic}")
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i+self.batch_size]
            for record in batch:
                self._send_data_with_retry(topic, record)
            self.logger.info(f"Sent batch {i // self.batch_size + 1} to {topic}")
            time.sleep(self.throttle_time)

    def close(self):
        self.producer.flush()
        self.producer.close()        
