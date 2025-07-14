import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from sqlalchemy import create_engine


class DataConnector:
    def __init__(self, postgres_uri: str, mongo_uri: str, mysql_uri: str, mongo_db_name: str):
        self.postgres_engine = create_engine(postgres_uri)
        self.mongo_client = MongoClient(mongo_uri)
        self.mysql_engine = create_engine(mysql_uri)
        self.mongo_db = self.mongo_client[mongo_db_name]

    def connect_postgres(self):
        return self.postgres_engine.connect()

    def connect_mongo(self):
        return self.mongo_db  

    def connect_mysql(self):
        return self.mysql_engine.connect()

    def close_connections(self):
        self.mongo_client.close()
        self.postgres_engine.dispose()
        self.mysql_engine.dispose()
