from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime
from typing import Literal
from datex.conversion.strategies import ConversionStrategy
from pathlib import Path


class PartType(str, Enum):
    IMG = "img"
    TEXT = "text"


class Part(BaseModel):
    type: PartType
    content: str
    metadata: dict = Field(default={})


class ConvertedFile(BaseModel):
    file_path: Path
    mime_type: str
    parts: list[Part]


class ConversionResult(BaseModel):
    status: Literal["pending", "running", "failed", "success"]
    duration: int
    files: list[ConvertedFile]
    errors: list[str]


class ConversionTask(BaseModel):
    file_paths: list[Path]
    requested_at: datetime
    strategy: ConversionStrategy
