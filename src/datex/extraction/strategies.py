from ollama import AsyncClient
from openai import AsyncOpenAI
from datex.extraction.schemas import ExtractionConfig
from datex.conversion.schemas import InputData, InputType

from typing import Protocol, Any


class ExtractionStrategy(Protocol):
    async def __call__(
        self,
        config: ExtractionConfig,
        input_data: list[InputData],
        output_schema: dict[str, Any],
    ) -> str: ...


# TODO: correctly convert text input_data


async def extract_with_ollama(
    config: ExtractionConfig,
    input_data: list[InputData],
    output_schema: dict[str, Any],
) -> str:
    ollama_response = await AsyncClient().chat(
        model=config.model_name,
        messages=[
            {"role": "system", "content": config.system_prompt},
            {
                "role": "user",
                "content": config.user_prompt,
                "images": [
                    i.input_content for i in input_data if i.input_type == InputType.IMG
                ],
            },
        ],
        stream=False,
        format=output_schema,
        options={"temperature": config.temperature, "top_p": config.top_p},
    )

    return ollama_response["message"]["content"] or ""


def _create_openai_user_prompt(user_prompt: str, input_data: list[InputData]):
    user_content = []
    user_content.append({"type": "input_text", "text": user_prompt})
    user_content.extend(
        [
            {
                "type": "input_image",
                "image_url": f"data:image/png;base64,{i.input_content}",
            }
            for i in input_data
            if i.input_type == InputType.IMG
        ]
    )
    return user_content


async def extract_with_openai(
    config: ExtractionConfig,
    input_data: list[InputData],
    output_schema: dict[str, Any],
) -> str:
    user_content = _create_openai_user_prompt(config.user_prompt, input_data)
    client = AsyncOpenAI(api_key=config.api_key)

    response = await client.responses.create(
        model=config.model_name,
        input=[
            {"role": "system", "content": config.system_prompt},
            {"role": "user", "content": user_content},
        ],
        temperature=config.temperature,
        top_p=config.top_p,
        text={
            "format": {
                "type": "json_schema",
                "name": "product_data",
                "schema": output_schema,
            }
        },
    )
    return response.output_text or ""
