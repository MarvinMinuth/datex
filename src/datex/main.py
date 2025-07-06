from dotenv import load_dotenv
from pdf2image import convert_from_path
from pathlib import Path
import json
from datex.extraction import extract_with_ollama, extract_with_openai
from datex.strategies.schemas import InputData, InputType
from concurrent.futures import ThreadPoolExecutor
import base64
from io import BytesIO
from datex.models.schemas import ModelConfig, Provider
import os

# TODO: move conversion part into own file

# TODO: add support for directories

# TODO: add expected output

load_dotenv()

def encode_page(page):
    byte_io = BytesIO()
    page.save(byte_io, format="PNG")
    base64_data = base64.b64encode(byte_io.getvalue()).decode("utf-8")
    return base64_data


def convert_to_b64images(pdf_path):
    images = convert_from_path(
        pdf_path=pdf_path,
        thread_count=5
    )

    b64_images = []
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(encode_page, images))
        b64_images.extend(results)

    return b64_images


def load_schema(
    path: Path,
): 
    with path.open() as f:
        schema = f.read()
    return json.loads(schema)


def convert():
    config = ModelConfig(
        provider=Provider.OPENAI,
        model_name="gpt-4.1-mini",
        temperature=0.1,
        top_p=0.1,
        api_key=os.getenv("OPENAI_API_KEY") # type: ignore
    )

    pdf_path = "./data/ExchangeServerToolbox_Data_Sheet_DE.pdf"
    print("Converting...")
    input_data = [InputData(input_type=InputType.IMG, input_content=img) for img in convert_to_b64images(pdf_path)]

    system_prompt = "The Assistant is Datex, an AI assistant to extract data from documents."
    output_schema = load_schema(Path("./output_schema.json"))
    user_prompt = "Extract the information from this data sheet. If a value is not present, provide null. Dates should be in the format YYYY-MM."

    print("Extracting...")
    params = {
        "config": config,
        "input_data": input_data,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "output_schema": output_schema
        }

    if config.provider == Provider.OLLAMA:
        result = extract_with_ollama(**params)
    else:
        result = extract_with_openai(**params)

    return result



if __name__ == "__main__":
    print(convert())
