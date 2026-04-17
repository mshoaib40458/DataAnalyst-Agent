from sqlalchemy import Column, String, Text, Integer, DateTime
from datetime import datetime, timezone
from app.db.database import Base


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id = Column(String, primary_key=True, index=True)
    filename = Column(String)
    file_path = Column(String)
    status = Column(String, default="uploaded")
    progress_logs = Column(Text, default="")
    error_message = Column(Text, default="")
    result_path = Column(String, nullable=True)
    analytical_questions = Column(Text, nullable=True)
    analysis_plan = Column(Text, nullable=True)
    cleaning_plan = Column(Text, nullable=True)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )


class AnalysisMemory(Base):
    __tablename__ = "analysis_memory"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    job_id = Column(String, index=True, nullable=False)
    schema_fingerprint = Column(String, index=True, nullable=False)
    insights_summary = Column(Text, nullable=False)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
