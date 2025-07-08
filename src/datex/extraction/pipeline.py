from pathlib import Path
from datex.extraction.schemas import ExtractionConfig, Provider
import json
from datex.extraction.strategies import (
    extract_with_ollama,
    extract_with_openai,
    ExtractionStrategy,
)
import asyncio


EXTRACTION_STRATEGIES: dict[Provider, ExtractionStrategy] = {
    Provider.OLLAMA: extract_with_ollama,
    Provider.OPENAI: extract_with_openai,
}


async def run_extractions(
    output_schema_path: Path,
    config: ExtractionConfig,
    conversion_result: dict,
) -> dict:
    print("Extracting...")
    with open(output_schema_path, "r") as file:
        output_schema = json.load(file)

    try:
        extract_func = EXTRACTION_STRATEGIES[config.provider]
    except KeyError:
        raise ValueError(f"Provider {config.provider} not supported.")

    params = {
        "config": config,
        "output_schema": output_schema,
    }

    extraction_tasks: dict[str, asyncio.Task] = {
        file: asyncio.create_task(extract_func(input_data=input_data, **params))
        for file, input_data in conversion_result.items()
    }

    await asyncio.gather(*extraction_tasks.values())
    extraction_results = {}

    for file, task in extraction_tasks.items():
        try:
            result = task.result()
            extraction_results[file] = json.loads(result)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for {file}: {e}")
            extraction_results[file] = {"error": "Invalid JSON response"}
        except Exception as e:
            print(f"An error occurred during extraction for {file}: {e}")
            extraction_results[file] = {"error": str(e)}

    return extraction_results
