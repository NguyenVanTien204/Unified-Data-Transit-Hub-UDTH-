# test connection script
import pandas as pd
from manual_extractor import ManualExtractor
import json
def test_connection(postgres_uri: str, mongo_uri: str, mysql_uri: str, mongo_db_name: str):
    extractor = None
    try:
        extractor = ManualExtractor(postgres_uri, mongo_uri, mysql_uri, mongo_db_name)

        # Extract PostgreSQL data
        postgres_data = extractor.extract_postgres_data()
        print("✅ PostgreSQL Data Extracted Successfully:")
        for key, df in postgres_data.items():
            print(f"Table: {key} — {df.shape[0]} rows")
            print(df.head(3))  
            print("-" * 50)

        # Extract MySQL data
        mysql_data = extractor.extract_mysql_data()
        print("✅ MySQL Data Extracted Successfully:")
        for key, df in mysql_data.items():
            print(f"Table: {key} — {df.shape[0]} rows")
            print(df.head(3)) 
            print("-" * 50)

        # Extract MongoDB data
        mongo_data = extractor.extract_mongo_data()
        print("✅ MongoDB Data Extracted Successfully:")
        for key, docs in mongo_data.items():
            print(f"Collection: {key} — {len(docs)} documents")
            for i, doc in enumerate(docs[:3]):  
                print(json.dumps(doc, indent=2, default=str))  
            print("-" * 50)

    except Exception as e:
        import traceback
        print(f"❌ An error occurred: {e}")
        traceback.print_exc()
    finally:
        if extractor and hasattr(extractor, 'connector') and extractor.connector:
            extractor.connector.close_connections()



test_connection(
    postgres_uri="postgresql+psycopg2://postgres:141124@localhost:5432/userdb",
    mongo_uri="mongodb://localhost:27017/",
    mysql_uri="mysql+pymysql://root:141124@localhost:3306/ordersdb",
    mongo_db_name="mock_shop"  
)