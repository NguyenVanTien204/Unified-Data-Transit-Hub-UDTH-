from kafka import KafkaProducer
import json
import yaml
import logging
import time
import kafka.errors as kafka_errors


class KafkaDataSender:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
    def send_data(self,df, topic: str):
        for _, row in df.iterrows():
            self.producer.send(topic, row.to_dict())
            time.sleep(0.1)  # Optional: sleep to avoid overwhelming the broker

    def send_documents(self, documents: list, topic: str):
        for doc in documents:
            self.producer.send(topic, doc)
            time.sleep(0.1)        

    def close(self):
        self.producer.flush()
        self.producer.close()
