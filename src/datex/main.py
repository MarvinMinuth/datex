from dotenv import load_dotenv
from pathlib import Path
import json
from datex.extraction import extract_with_ollama, extract_with_openai
from datex.conversion.image_per_page import convert
from datex.models.schemas import ModelConfig, Provider
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import asyncio
import argparse

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("config_path")
parser.add_argument("output_schema_path")
parser.add_argument("dataset_path")


def load_config(path: Path) -> tuple[ModelConfig, str, str]:
    with open(path, "r") as file:
        config_file = json.load(file)

    api_key = config_file.get("api_key") or os.getenv("OPENAI_API_KEY") or ""

    config = ModelConfig(
        provider=config_file["provider"],
        model_name=config_file["model_name"],
        temperature=config_file["temperature"],
        top_p=config_file["top_p"],
        api_key=api_key,
    )

    system_prompt = config_file.get(
        "system_prompt",
        "The Assistant is Datex, an AI assistant to extract data from documents.",
    )
    user_prompt = config_file.get(
        "user_prompt",
        "Extract the information from this data sheet. If a value is not present, provide null. Dates should be in the format YYYY-MM.",
    )
    return (config, system_prompt, user_prompt)


def prepare_dataset(path: Path) -> tuple[list[Path], dict | None]:
    pdf_paths = []
    expected_output = None
    for file in path.iterdir():
        if file.suffix == ".pdf":
            pdf_paths.append(Path(file))
        elif file.name == "expected_output.json":
            with open(file, mode="r", encoding="utf-8") as f:
                expected_output = json.load(f)
    return (pdf_paths, expected_output)


def run_conversions(paths: list[Path]) -> dict:
    print("Converting...")
    conversion_result = {}
    with ThreadPoolExecutor() as executor:
        conversion_tasks = {
            executor.submit(convert, file_path): file_path.name for file_path in paths
        }
        for future in concurrent.futures.as_completed(conversion_tasks):
            converted_file = conversion_tasks[future]
            try:
                conversion_result[converted_file] = future.result()
            except Exception as exc:
                print(f"{converted_file} caused an exception: {exc}")
    return conversion_result


async def run_extractions(
    output_schema_path: Path,
    config: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    conversion_result: dict,
) -> dict:
    print("Extracting...")
    with open(output_schema_path, "r") as file:
        output_schema = json.load(file)

    extract_func = None

    if config.provider == Provider.OLLAMA:
        extract_func = extract_with_ollama
    else:
        extract_func = extract_with_openai

    params = {
        "config": config,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "output_schema": output_schema,
    }

    assert extract_func is not None
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


async def run_pipeline(config_path: Path, output_schema_path: Path, dataset_path: Path):
    config, system_prompt, user_prompt = load_config(path=config_path)
    pdf_paths, expected_output = prepare_dataset(dataset_path)

    conversion_result = run_conversions(paths=pdf_paths)
    extraction_results = await run_extractions(
        output_schema_path=output_schema_path,
        config=config,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        conversion_result=conversion_result,
    )

    return (extraction_results, expected_output)


async def main():
    args = parser.parse_args()
    config_path = Path(args.config_path)
    output_schema_path = Path(args.output_schema_path)
    dataset_path = Path(args.dataset_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config File not found: {config_path}")
    if not output_schema_path.exists():
        raise FileNotFoundError(f"Output Schema JSON not found: {output_schema_path}")
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    result = await run_pipeline(
        config_path=config_path,
        output_schema_path=output_schema_path,
        dataset_path=dataset_path,
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
