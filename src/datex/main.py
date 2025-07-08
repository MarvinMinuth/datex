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

# TODO: add expected output

# TODO: move config and prompts to json files

load_dotenv()

async def extract():
    with open("./config.json", "r") as file:
        config_file = json.load(file)

    api_key = (
        config_file.get("api_key")
        or os.getenv("OPENAI_API_KEY")
        or ""
    )

    config = ModelConfig(
        provider=config_file["provider"],
        model_name=config_file["model_name"],
        temperature=config_file["temperature"],
        top_p=config_file["top_p"],
        api_key=api_key
    )

    system_prompt = config_file.get("system_prompt", "The Assistant is Datex, an AI assistant to extract data from documents.")
    user_prompt = config_file.get(
            "user_prompt", 
            "Extract the information from this data sheet. If a value is not present, provide null. Dates should be in the format YYYY-MM."
    )

    dataset_path = Path(config_file["dataset_path"])
    pdf_paths = []
    for file in dataset_path.iterdir():
        if file.suffix == ".pdf":
            pdf_paths.append(Path(file))
    print("Converting...")
    conversion_result = {}
    with ThreadPoolExecutor() as executor:
        conversion_tasks = {executor.submit(convert, file_path): file_path.name for file_path in pdf_paths}
        for future in concurrent.futures.as_completed(conversion_tasks):
            converted_file = conversion_tasks[future]
            try:
                conversion_result[converted_file] = future.result()
            except Exception as exc:
                print(f"{converted_file} caused an exception: {exc}")

    print("Extracting...")
    with open("./output_schema.json", "r") as file:
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
            "output_schema": output_schema
            }

    assert extract_func is not None
    extraction_tasks: dict[str, asyncio.Task] = {
        file: asyncio.create_task(extract_func(input_data=input_data, **params))
        for file, input_data in conversion_result.items()
    }

    await asyncio.gather(*extraction_tasks.values())
    extraction_results = {
        file: task.result()
        for file, task in extraction_tasks.items()
    }

    print(extraction_results)
    return(extraction_results)
    
if __name__ == "__main__":
    asyncio.run(extract())
