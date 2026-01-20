from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, schemas

app = FastAPI(title="Medical Data Warehouse API")

@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(limit: int = 10, db: Session = Depends(database.get_db)):
    # Simple keyword frequency as query proxy for "products"
    result = db.execute(text("""
        SELECT 
            word as product_name, 
            nentry as mention_count
        FROM ts_stat('SELECT to_tsvector(''english'', message_text) FROM raw.telegram_messages')
        ORDER BY mention_count DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()
    return result

@app.get("/api/channels/{channel_name}/activity", response_model=schemas.ChannelActivity)
def get_channel_activity(channel_name: str, db: Session = Depends(database.get_db)):
    result = db.execute(text("""
        SELECT 
            DATE(date) as date,
            COUNT(*) as post_count
        FROM raw.telegram_messages
        WHERE channel_name = :channel_name
        GROUP BY DATE(date)
        ORDER BY date
    """), {"channel_name": channel_name}).fetchall()
    
    if not result:
        raise HTTPException(status_code=404, detail="Channel not found")
        
    return {
        "channel_name": channel_name,
        "activity": [{"date": r.date, "post_count": r.post_count} for r in result]
    }

@app.get("/api/search/messages", response_model=List[schemas.Message])
def search_messages(query: str, limit: int = 20, db: Session = Depends(database.get_db)):
    result = db.execute(text("""
        SELECT 
            m.message_id,
            m.channel_name,
            m.date,
            m.message_text as text,
            m.views,
            c.category as image_category
        FROM raw.telegram_messages m
        LEFT JOIN raw.image_categories c ON m.message_id = c.message_id
        WHERE m.message_text ILIKE :query
        LIMIT :limit
    """), {"query": f"%{query}%", "limit": limit}).fetchall()
    
    return result

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualStats])
def get_visual_content_stats(db: Session = Depends(database.get_db)):
    # This assumes fct_image_detections exists and is populated
    try:
        result = db.execute(text("""
            SELECT
                c.channel_name,
                COUNT(DISTINCT f.message_id) as total_images,
                COUNT(DISTINCT CASE WHEN f.image_category = 'promotional' THEN f.message_id END) as promotional_images,
                COUNT(DISTINCT CASE WHEN f.image_category = 'product_display' THEN f.message_id END) as product_images,
                COUNT(DISTINCT CASE WHEN f.image_category = 'lifestyle' THEN f.message_id END) as lifestyle_images,
                COUNT(DISTINCT CASE WHEN f.image_category NOT IN ('promotional', 'product_display', 'lifestyle') OR f.image_category IS NULL THEN f.message_id END) as other_images
            FROM public_marts.fct_image_detections f
            JOIN public_marts.dim_channels c ON f.channel_key = c.channel_key
            GROUP BY c.channel_name
        """)).fetchall()
        return result
    except Exception as e:
        # Fallback if mart not ready
        raise HTTPException(status_code=503, detail=f"Analytics data not available yet. Error: {e}")
