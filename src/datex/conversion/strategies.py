from enum import Enum
from typing import Protocol, Type
from datex.conversion.schemas import (
    ConversionTask,
    ConversionResult,
    Part,
    PartType,
    ConvertedFile,
)
from io import BytesIO
import base64
from concurrent.futures import ThreadPoolExecutor
from pdf2image import convert_from_path
from pathlib import Path
import concurrent.futures


class Conversion(Protocol):
    task: ConversionTask

    def __init__(self, task: ConversionTask) -> None:
        self.task = task

    def __call__(self) -> ConversionResult: ...


class ImgPerPageConversion(Conversion):
    def __call__(self) -> ConversionResult:
        converted_files: list[ConvertedFile] = []
        errors = []
        with ThreadPoolExecutor() as executor:
            future_to_file_path = {
                executor.submit(self._convert, Path(file_path)): file_path
                for file_path in self.task.file_paths
            }
            for future in concurrent.futures.as_completed(future_to_file_path):
                file_path = future_to_file_path[future]
                try:
                    data = future.result()
                    converted_files.append(
                        ConvertedFile(file_path=file_path, mime_type="png", parts=data)
                    )
                except Exception as exc:
                    errors.append(f"Error converting {file_path}: {exc}")

        return ConversionResult(
            status="success", duration=0, files=converted_files, errors=errors
        )

    def _encode_page(self, page):
        byte_io = BytesIO()
        page.save(byte_io, format="PNG")
        base64_data = base64.b64encode(byte_io.getvalue()).decode("utf-8")
        return base64_data

    def _convert_to_b64images(self, pdf_path):
        images = convert_from_path(pdf_path=pdf_path, thread_count=5)

        b64_images = []
        for image in images:
            b64_images.append(self._encode_page(image))

        return b64_images

    def _convert(self, pdf_path: Path) -> list[Part]:
        input_data = [
            Part(type=PartType.IMG, content=img)
            for img in self._convert_to_b64images(pdf_path)
        ]
        return input_data


class ConversionStrategy(Enum):
    PDF2IMG = ("pdf2img", ImgPerPageConversion)

    def __init__(self, value: str, strategy_class: Type[Conversion]):
        self._value_ = value
        self.strategy_class = strategy_class
