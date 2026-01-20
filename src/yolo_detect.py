import os
import glob
import pandas as pd
import psycopg2
from ultralytics import YOLO
from dotenv import load_dotenv

load_dotenv()

# Database connection details
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

def create_detection_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                id SERIAL PRIMARY KEY,
                image_path TEXT,
                detected_class VARCHAR(50),
                confidence DOUBLE PRECISION,
                x1 DOUBLE PRECISION,
                y1 DOUBLE PRECISION,
                x2 DOUBLE PRECISION,
                y2 DOUBLE PRECISION,
                detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def run_detection(model_path='yolov8n.pt', images_dir='data/raw/images'):
    model = YOLO(model_path)
    
    # Recursively find all jpg images
    image_paths = glob.glob(os.path.join(images_dir, '**', '*.jpg'), recursive=True)
    
    detections = []
    
    print(f"Found {len(image_paths)} images to process.")
    
    for img_path in image_paths:
        try:
            results = model(img_path, verbose=False)
            
            # If no detections
            if not results or len(results[0].boxes) == 0:
                print(f"No detections for {img_path}")
                detections.append({
                    'image_path': img_path,
                    'detected_class': None,
                    'confidence': 0.0,
                    'x1': 0.0,
                    'y1': 0.0,
                    'x2': 0.0,
                    'y2': 0.0
                })
                continue

            for result in results:
                for box in result.boxes:
                    cls_id = int(box.cls[0])
                    class_name = model.names[cls_id]
                    conf = float(box.conf[0])
                    xyxy = box.xyxy[0].tolist()
                    
                    detections.append({
                        'image_path': img_path,
                        'detected_class': class_name,
                        'confidence': conf,
                        'x1': xyxy[0],
                        'y1': xyxy[1],
                        'x2': xyxy[2],
                        'y2': xyxy[3]
                    })
        except Exception as e:
            print(f"Error processing {img_path}: {e}")

    return detections

def save_to_db(detections):
    if not detections:
        print("No detections to save.")
        return

    conn = get_db_connection()
    create_detection_table(conn)
    
    with conn.cursor() as cur:
        # Use simple insert for now, can be optimized with copy_from
        for d in detections:
            cur.execute("""
                INSERT INTO raw.yolo_detections 
                (image_path, detected_class, confidence, x1, y1, x2, y2)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                d['image_path'],
                d['detected_class'],
                d['confidence'],
                d['x1'],
                d['y1'],
                d['x2'],
                d['y2']
            ))
        conn.commit()
    conn.close()
    print(f"Saved {len(detections)} detection records to database.")

def save_to_csv(detections, output_path='data/processed/yolo_detections.csv'):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.DataFrame(detections)
    df.to_csv(output_path, index=False)
    print(f"Saved detections to {output_path}")

if __name__ == "__main__":
    detections = run_detection()
    save_to_csv(detections)
    save_to_db(detections)
