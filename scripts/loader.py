import os
import json
import glob
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'medical_warehouse')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_raw_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                channel_name VARCHAR(255),
                message_id BIGINT,
                date TIMESTAMP,
                message_text TEXT,
                views INTEGER,
                forwards INTEGER,
                media_path TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def load_data(conn):
    base_path = "data/raw/telegram_messages"
    json_files = glob.glob(f"{base_path}/*/*.json")
    
    with conn.cursor() as cur:
        for file_path in json_files:
            # Extract channel name from filename (e.g. "channel_name.json")
            channel_name = os.path.basename(file_path).replace('.json', '')
            
            with open(file_path, 'r', encoding='utf-8') as f:
                messages = json.load(f)
                
            for msg in messages:
                cur.execute("""
                    INSERT INTO raw.telegram_messages 
                    (channel_name, message_id, date, message_text, views, forwards, media_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING; -- Assuming simple dedup logic for now if PK was composite
                """, (
                    channel_name,
                    msg['id'],
                    msg['date'],
                    msg['text'],
                    msg['views'],
                    msg['forwards'],
                    msg['media_path']
                ))
            print(f"Loaded {len(messages)} messages from {file_path}")
        conn.commit()

if __name__ == "__main__":
    try:
        conn = get_db_connection()
        create_raw_schema(conn)
        load_data(conn)
        conn.close()
        print("Data loading complete.")
    except Exception as e:
        print(f"Error loading data: {e}")
