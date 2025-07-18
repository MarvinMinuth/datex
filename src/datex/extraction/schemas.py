from pydantic import BaseModel, Field, model_validator, ConfigDict
from enum import Enum
import os
from typing import Any, Dict, Literal
from datex.conversion.schemas import ConvertedFile


class Provider(str, Enum):
    OPENAI = "openai"
    OLLAMA = "ollama"


class ExtractionConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    provider: Provider
    model_name: str
    system_prompt: str
    user_prompt: str
    temperature: float = Field(lt=1, gt=0)
    top_p: float = Field(lt=1, gt=0)
    api_key: str = Field(default="")

    @model_validator(mode="after")
    def check_for_api_key(self):
        none_key_provider = [Provider.OLLAMA]
        if not self.api_key and self.provider not in none_key_provider:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.api_key = api_key
            else:
                raise ValueError(
                    f"API Key must be set if provider none of: {none_key_provider}"
                )
        return self


class ExtractedFile(BaseModel):
    file_path: str
    data: Dict[str, Any] | None = None
    error: str | None = None


class ExtractionResult(BaseModel):
    status: Literal["pending", "running", "failed", "success"]
    duration: int
    files: list[ExtractedFile]


class ExtractionTask(BaseModel):
    config: ExtractionConfig
    output_schema: Dict[str, Any]
    files: list[ConvertedFile]
