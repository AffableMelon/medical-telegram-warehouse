from pydantic import BaseModel
from typing import List, Optional, Any
from datetime import datetime

class TopProduct(BaseModel):
    product_name: str
    mention_count: int

class DailyActivity(BaseModel):
    date: datetime
    post_count: int

class ChannelActivity(BaseModel):
    channel_name: str
    activity: List[DailyActivity]

class Message(BaseModel):
    message_id: int
    channel_name: str
    date: datetime
    text: str
    views: int
    image_category: Optional[str] = None

class VisualStats(BaseModel):
    channel_name: str
    total_images: int
    promotional_images: int
    product_images: int
    lifestyle_images: int
    other_images: int
