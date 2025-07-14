# manual data extractor
import pandas as pd
from connector import DataConnector


class ManualExtractor:
    def __init__(self, postgres_uri: str, mongo_uri: str, mysql_uri: str, mongo_db_name: str):
        self.connector = DataConnector(postgres_uri, mongo_uri, mysql_uri, mongo_db_name)

    def extract_postgres_data(self):
        self.connector.connect_postgres()
        interactions_df = pd.read_sql("SELECT * FROM interactions", self.connector.postgres_engine)
        user_notifications_df = pd.read_sql("SELECT * FROM user_notifications", self.connector.postgres_engine)
        user_preferences_df = pd.read_sql("SELECT * FROM user_preferences", self.connector.postgres_engine)
        user_sessions_df = pd.read_sql("SELECT * FROM user_sessions", self.connector.postgres_engine)
        user_df = pd.read_sql("SELECT * FROM users", self.connector.postgres_engine)
        # return to transform data
        return {
            "interactions": interactions_df,
            "user_notifications": user_notifications_df,
            "user_preferences": user_preferences_df,
            "user_sessions": user_sessions_df,
            "users": user_df
        }
    
    def extract_mysql_data(self):
        self.connector.connect_mysql()
        order_items_df = pd.read_sql("SELECT * FROM order_items", self.connector.mysql_engine)
        orders_df = pd.read_sql("SELECT * FROM orders", self.connector.mysql_engine)
        products_df = pd.read_sql("SELECT * FROM products", self.connector.mysql_engine)
        transactions_df = pd.read_sql("SELECT * FROM transactions", self.connector.mysql_engine)
        users_df = pd.read_sql("SELECT * FROM users", self.connector.mysql_engine)
        # return to transform data
        return {
            "order_items": order_items_df,
            "orders": orders_df,
            "products": products_df,
            "transactions": transactions_df,
            "users": users_df
        }
    
    def extract_mongo_data(self):
        mongo_db = self.connector.connect_mongo()
        target_collection = ['analytics', 'products', 'ratings', 'stock_levels', 'users_messages']
        mongo_data = {}
        for collection in target_collection:
            if collection in mongo_db.list_collection_names():
                cursor = mongo_db[collection].find({})
                mongo_data[collection] = list(cursor)
        return mongo_data
