from ollama import AsyncClient
from openai import AsyncOpenAI
from datex.extraction.schemas import ExtractionConfig, Provider
from datex.conversion.schemas import Part, PartType
from enum import Enum
from typing import Protocol, Any, Type


class Extraction(Protocol):
    def __init__(self, config: ExtractionConfig, output_schema: dict[str, Any]): ...
    async def __call__(self, input_data: list[Part]) -> str: ...


class OpenAIStrategy(Extraction):
    def __init__(
        self,
        config: ExtractionConfig,
        output_schema: dict[str, Any],
    ):
        self.config = config
        self.output_schema = output_schema

        self.client = AsyncOpenAI(api_key=config.api_key)

    def _create_openai_user_prompt(self, input_data):
        user_content = []
        user_content.append({"type": "input_text", "text": self.config.user_prompt})
        user_content.extend(
            [
                {
                    "type": "input_image",
                    "image_url": f"data:image/png;base64,{i.input_content}",
                }
                for i in input_data
                if i.input_type == PartType.IMG
            ]
        )
        return user_content

    async def __call__(self, input_data: list[Part]) -> str:
        user_content = self._create_openai_user_prompt(input_data)

        response = await self.client.responses.create(
            model=self.config.model_name,
            input=[
                {"role": "system", "content": self.config.system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=self.config.temperature,
            top_p=self.config.top_p,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "product_data",
                    "schema": self.output_schema,
                }
            },
        )
        return response.output_text or ""


# TODO: correctly convert text input_data


class OllamaStrategy(Extraction):
    def __init__(
        self,
        config: ExtractionConfig,
        output_schema: dict[str, Any],
    ):
        self.config = config
        self.output_schema = output_schema

        self.client = AsyncClient()

    async def __call__(self, input_data: list[Part]) -> str:
        ollama_response = await self.client.chat(
            model=self.config.model_name,
            messages=[
                {"role": "system", "content": self.config.system_prompt},
                {
                    "role": "user",
                    "content": self.config.user_prompt,
                    "images": [i.content for i in input_data if i.type == PartType.IMG],
                },
            ],
            stream=False,
            format=self.output_schema,
            options={
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
            },
        )

        return ollama_response["message"]["content"] or ""


class ExtractionStrategy(Enum):
    OPENAI = (Provider.OPENAI, OpenAIStrategy)
    OLLAMA = (Provider.OLLAMA, OllamaStrategy)

    def __init__(self, provider: Provider, strategy_class: Type[Extraction]):
        self._value_ = provider
        self.strategy_class = strategy_class
