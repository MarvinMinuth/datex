from dotenv import load_dotenv
from pathlib import Path
import json
from datex.extraction import extract_with_ollama, extract_with_openai
from datex.conversion.image_per_page import convert
from datex.models.schemas import ModelConfig, Provider
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures


# TODO: add support for directories

# TODO: add expected output

# TODO: move config and prompts to json files

load_dotenv()

def load_schema(
    path: Path,
): 
    with path.open() as f:
        schema = f.read()
    return json.loads(schema)


def extract():
    config = ModelConfig(
        provider=Provider.OPENAI,
        model_name="gpt-4.1-mini",
        temperature=0.1,
        top_p=0.1,
        api_key=os.getenv("OPENAI_API_KEY") # type: ignore
    )

    system_prompt = "The Assistant is Datex, an AI assistant to extract data from documents."
    output_schema = load_schema(Path("./output_schema.json"))
    user_prompt = "Extract the information from this data sheet. If a value is not present, provide null. Dates should be in the format YYYY-MM."

    dataset_path = Path("./data")
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
    extraction_result = {}

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


    with ThreadPoolExecutor() as executor:
        extraction_tasks = {executor.submit(extract_func, input_data=input_data, **params): file for file, input_data in conversion_result.items()}
        for future in concurrent.futures.as_completed(extraction_tasks):
            extracted_file = extraction_tasks[future]
            try:
                extraction_result[extracted_file] = future.result()
            except Exception as exc:
                print(f"{extracted_file} caused an exception: {exc}")
        

    return extraction_result


if __name__ == "__main__":
    print(extract())
