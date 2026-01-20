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
        # Create detections table with message_id and channel_name
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                id SERIAL PRIMARY KEY,
                image_path TEXT,
                message_id BIGINT,
                channel_name TEXT,
                detected_class VARCHAR(50),
                confidence DOUBLE PRECISION,
                x1 DOUBLE PRECISION,
                y1 DOUBLE PRECISION,
                x2 DOUBLE PRECISION,
                y2 DOUBLE PRECISION,
                detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Create categories table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.image_categories (
                message_id BIGINT PRIMARY KEY,
                channel_name TEXT,
                image_path TEXT,
                category VARCHAR(50),
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def classify_detections(detections_list):
    """
    Explicit categorization logic.
    """
    classes = set(d['detected_class'] for d in detections_list if d['detected_class'])
    
    if 'person' in classes and ('bottle' in classes or 'cup' in classes): # Expanded logic example
        return 'promotional'
    elif 'bottle' in classes or 'cup' in classes:
        return 'product_display'
    elif 'person' in classes:
        return 'lifestyle'
    elif not classes:
        return 'no_content'
    else:
        return 'miscellaneous'

def run_detection(model_path='yolov8n.pt', images_dir='data/raw/images'):
    model = YOLO(model_path)
    
    # Recursively find all jpg images
    image_paths = glob.glob(os.path.join(images_dir, '**', '*.jpg'), recursive=True)
    
    all_detections = []
    image_categories = []
    
    print(f"Found {len(image_paths)} images to process.")
    
    for img_path in image_paths:
        try:
            # Extract metadata from path
            # Expected format: data/raw/images/{channel_name}/{message_id}.jpg
            parts = img_path.split(os.sep)
            if len(parts) >= 2:
                channel_name = parts[-2]
                filename = parts[-1]
                try:
                    message_id = int(filename.split('.')[0])
                except ValueError:
                    print(f"Skipping {img_path}: Filename is not a valid message_id")
                    continue
            else:
                print(f"Skipping {img_path}: path structure unknown")
                continue

            results = model(img_path, verbose=False)
            
            current_image_detections = []

            # If no detections
            if not results or len(results[0].boxes) == 0:
                print(f"No detections for {img_path}")
                # We still might want to categorize it as 'no_content'
                pass
            else:
                for result in results:
                    for box in result.boxes:
                        cls_id = int(box.cls[0])
                        class_name = model.names[cls_id]
                        conf = float(box.conf[0])
                        xyxy = box.xyxy[0].tolist()
                        
                        det = {
                            'image_path': img_path,
                            'message_id': message_id,
                            'channel_name': channel_name,
                            'detected_class': class_name,
                            'confidence': conf,
                            'x1': xyxy[0],
                            'y1': xyxy[1],
                            'x2': xyxy[2],
                            'y2': xyxy[3]
                        }
                        current_image_detections.append(det)
                        all_detections.append(det)

            # Classify image
            category = classify_detections(current_image_detections)
            image_categories.append({
                'message_id': message_id,
                'channel_name': channel_name,
                'image_path': img_path,
                'category': category
            })
            
        except Exception as e:
            print(f"Error processing {img_path}: {e}")

    return all_detections, image_categories

def save_to_db(detections, categories):
    conn = get_db_connection()
    create_detection_table(conn)
    
    with conn.cursor() as cur:
        # Save detections
        if detections:
            args_list = [
                (d['image_path'], d['message_id'], d['channel_name'], d['detected_class'], 
                 d['confidence'], d['x1'], d['y1'], d['x2'], d['y2']) 
                for d in detections
            ]
            # Use executemany for bulk insert
            cur.executemany("""
                INSERT INTO raw.yolo_detections 
                (image_path, message_id, channel_name, detected_class, confidence, x1, y1, x2, y2)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, args_list)
        
        # Save categories (Upsert to handle re-runs)
        if categories:
            args_list_cat = [
                (c['message_id'], c['channel_name'], c['image_path'], c['category'])
                for c in categories
            ]
            cur.executemany("""
                INSERT INTO raw.image_categories (message_id, channel_name, image_path, category)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id) 
                DO UPDATE SET category = EXCLUDED.category, image_path = EXCLUDED.image_path, processed_date = CURRENT_TIMESTAMP;
            """, args_list_cat)
            
        conn.commit()
    conn.close()
    print(f"Saved {len(detections)} detections and {len(categories)} classifications to DB.")

def save_to_csv(detections, categories, output_path='data/processed'):
    os.makedirs(output_path, exist_ok=True)
    
    if detections:
        pd.DataFrame(detections).to_csv(f"{output_path}/yolo_detections.csv", index=False)
        
    if categories:
        pd.DataFrame(categories).to_csv(f"{output_path}/image_categories.csv", index=False)
        
    print(f"Saved CSVs to {output_path}")

if __name__ == "__main__":
    detections, categories = run_detection()
    save_to_csv(detections, categories)
    save_to_db(detections, categories)

