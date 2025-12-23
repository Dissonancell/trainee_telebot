import uuid
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, DateTime, UUID
from pydantic import BaseModel
from typing import List, Optional


# ------------------- База для SQLAlchemy -------------------
class Base(AsyncAttrs, DeclarativeBase):
    pass

# ------------------- Таблицы -------------------
class Video(Base):
    __tablename__ = "videos"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    creator_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    video_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    views_count: Mapped[int]
    likes_count: Mapped[int]
    comments_count: Mapped[int]
    reports_count: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    snapshots: Mapped[List["VideoSnapshot"]] = relationship(back_populates="video")

class VideoSnapshot(Base):
    __tablename__ = "video_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    video_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("videos.id"))
    views_count: Mapped[int]
    likes_count: Mapped[int]
    comments_count: Mapped[int]
    reports_count: Mapped[int]
    delta_views_count: Mapped[int]
    delta_likes_count: Mapped[int]
    delta_comments_count: Mapped[int]
    delta_reports_count: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    video: Mapped[Video] = relationship(back_populates="snapshots")

# ------------------- Pydantic схемы -------------------
class VideoSnapshotSchema(BaseModel):
    id: uuid.UUID
    video_id: uuid.UUID
    views_count: int
    likes_count: int
    comments_count: int
    reports_count: int
    delta_views_count: int
    delta_likes_count: int
    delta_comments_count: int
    delta_reports_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

class VideoSchema(BaseModel):
    id: uuid.UUID
    creator_id: uuid.UUID
    video_created_at: datetime
    views_count: int
    likes_count: int
    comments_count: int
    reports_count: int
    created_at: datetime
    updated_at: datetime
    snapshots: Optional[List[VideoSnapshotSchema]] = []

    model_config = {"from_attributes": True}
