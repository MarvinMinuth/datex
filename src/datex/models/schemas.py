from pydantic import BaseModel, Field, model_validator 
from enum import Enum

class Provider(str, Enum):
    OPENAI = 'openai'
    OLLAMA = 'ollama'


class ModelConfig(BaseModel):
    provider: Provider
    model_name: str
    temperature: float = Field(lt=1, gt=0)
    top_p: float = Field(lt=1, gt=0)
    api_key: str = Field(default="")

    @model_validator(mode="after")
    def check_for_api_key(self):
        none_key_provider = [Provider.OLLAMA]
        if not self.api_key and self.provider not in none_key_provider:
            raise ValueError(f"API Key must be set if provider none of: {none_key_provider}")
        return self
