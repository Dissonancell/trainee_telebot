import uuid
import os
import json
import asyncio
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Video, VideoSnapshot


load_dotenv()

DB_URL = os.getenv("DB_URL")
JSON_FILE = "videos.json"

# ------------------- Асинхронный движок и сессия -------------------
engine = create_async_engine(DB_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ------------------- Создание таблиц -------------------
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ------------------- Загрузка данных -------------------
async def load_json():
    async with AsyncSessionLocal() as session:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for v in data['videos']:
            video = Video(
                id=uuid.UUID(v['id']),
                creator_id=uuid.UUID(v['creator_id']),
                video_created_at=datetime.fromisoformat(v['video_created_at']),
                views_count=v['views_count'],
                likes_count=v['likes_count'],
                comments_count=v['comments_count'],
                reports_count=v['reports_count'],
                created_at=datetime.fromisoformat(v['created_at']),
                updated_at=datetime.fromisoformat(v['updated_at'])
            )
            session.add(video)
            for s in v.get('snapshots', []):
                snapshot = VideoSnapshot(
                    id=uuid.UUID(s['id']),
                    video_id=uuid.UUID(v['id']),
                    views_count=s['views_count'],
                    likes_count=s['likes_count'],
                    comments_count=s['comments_count'],
                    reports_count=s['reports_count'],
                    delta_views_count=s['delta_views_count'],
                    delta_likes_count=s['delta_likes_count'],
                    delta_comments_count=s['delta_comments_count'],
                    delta_reports_count=s['delta_reports_count'],
                    created_at=datetime.fromisoformat(s['created_at']),
                    updated_at=datetime.fromisoformat(s['updated_at'])
                )
                session.add(snapshot)
        await session.commit()

async def main():
    await init_db()
    await load_json()
    print("Данные успешно загружены.")

if __name__ == "__main__":
    asyncio.run(main())
