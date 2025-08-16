"""Simple end-to-end extract and push to Kafka for manual testing."""
import pandas as pd
from .manual_extractor import ManualExtractor
from pathlib import Path
from typing import Dict, List

# Import the Kafka sender
from extractors.kafka.kafka_producer import KafkaDataSender


def _send_postgres_blocks(sender: KafkaDataSender, postgres_data: Dict[str, pd.DataFrame]):
    for table, df in postgres_data.items():
        # Topic suffix like: postgres.users
        topic_suffix = f"postgres.{table}"
        sender.send_dataframe(df, topic_suffix)


def _send_mysql_blocks(sender: KafkaDataSender, mysql_data: Dict[str, pd.DataFrame]):
    for table, df in mysql_data.items():
        topic_suffix = f"mysql.{table}"
        sender.send_dataframe(df, topic_suffix)


def _send_mongo_blocks(sender: KafkaDataSender, mongo_data: Dict[str, List[dict]]):
    for collection, docs in mongo_data.items():
        topic_suffix = f"mongo.{collection}"
        sender.send_documents(docs, topic_suffix)


def test_connection(postgres_uri: str, mongo_uri: str, mysql_uri: str, mongo_db_name: str, kafka_config_path: str):
    extractor = None
    sender = None
    try:
        extractor = ManualExtractor(postgres_uri, mongo_uri, mysql_uri, mongo_db_name)
        sender = KafkaDataSender(kafka_config_path)

        # Extract and immediately push
        postgres_data = extractor.extract_postgres_data()
        _send_postgres_blocks(sender, postgres_data)

        mysql_data = extractor.extract_mysql_data()
        _send_mysql_blocks(sender, mysql_data)

        mongo_data = extractor.extract_mongo_data()
        _send_mongo_blocks(sender, mongo_data)

        print("✅ Extracted and sent all sources to Kafka.")

    except Exception as e:
        import traceback
        print(f"❌ An error occurred: {e}")
        traceback.print_exc()
    finally:
        if sender:
            try:
                sender.close()
            except Exception:
                pass
        if extractor and hasattr(extractor, 'connector') and extractor.connector:
            extractor.connector.close_connections()


if __name__ == "__main__":
    test_connection(
        postgres_uri="postgresql+psycopg2://postgres:141124@localhost:5432/userdb",
        mongo_uri="mongodb://localhost:27017/",
        mysql_uri="mysql+pymysql://root:141124@localhost:3306/ordersdb",
        mongo_db_name="mock_shop",
        kafka_config_path=str(Path(__file__).resolve().parents[1] / "kafka" / "kafka_config.yaml"),
    )
