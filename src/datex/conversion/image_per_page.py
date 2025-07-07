from datex.conversion.schemas import InputData, InputType
from io import BytesIO
import base64
from concurrent.futures import ThreadPoolExecutor
from pdf2image import convert_from_path
from pathlib import Path


def _encode_page(page):
    byte_io = BytesIO()
    page.save(byte_io, format="PNG")
    base64_data = base64.b64encode(byte_io.getvalue()).decode("utf-8")
    return base64_data


def _convert_to_b64images(pdf_path):
    images = convert_from_path(
        pdf_path=pdf_path,
        thread_count=5
    )

    b64_images = []
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_encode_page, images))
        b64_images.extend(results)

    return b64_images

def convert(pdf_path: Path) -> list[InputData]:
    input_data = [InputData(input_type=InputType.IMG, input_content=img) for img in _convert_to_b64images(pdf_path)]
    return input_data
