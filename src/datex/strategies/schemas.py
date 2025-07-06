from pydantic import BaseModel
from enum import Enum

class InputType(str,Enum):
    IMG = 'img'
    TEXT = 'text'

class InputData(BaseModel):
    input_type: InputType
    input_content: str


