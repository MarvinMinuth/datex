from dotenv import load_dotenv
from pathlib import Path
import json
from datex.extraction import run_extractions
from datex.extraction.schemas import ExtractionConfig, ExtractionTask
from datex.conversion import run_conversions
from datex.conversion.schemas import ConversionTask
from datex.conversion.strategies import ConversionStrategy
import asyncio
import argparse
from datetime import datetime

load_dotenv()


def load_config(path: Path) -> ExtractionConfig:
    with open(path, "r") as file:
        config_file = json.load(file)

    config = ExtractionConfig.model_validate(config_file)

    return config


def prepare_dataset(path: Path) -> list[Path]:
    pdf_paths = []
    for file in path.iterdir():
        if file.suffix == ".pdf":
            pdf_paths.append(file)
    return pdf_paths


async def run_pipeline(
    config_path: Path,
    output_schema_path: Path,
    dataset_path: Path,
    expected_result_path: Path,
):
    config = load_config(path=config_path)

    with open(expected_result_path, mode="r", encoding="utf-8") as file:
        expected_result = json.load(file)

    pdf_paths = prepare_dataset(path=dataset_path)
    conversion_task = ConversionTask(
        file_paths=pdf_paths,
        strategy=ConversionStrategy.PDF2IMG,
        requested_at=datetime.now(),
    )
    conversion_result = run_conversions(conversion_task)

    with open(output_schema_path, "r") as file:
        output_schema = json.load(file)

    extraction_task = ExtractionTask(
        config=config,
        output_schema=output_schema,
        files=conversion_result.files,
    )

    extraction_results = await run_extractions(task=extraction_task)

    return (extraction_results, expected_result)


def dir_path(str):
    path = Path(str)
    if not path.exists():
        raise NotADirectoryError(f"Directory not found: {path}")
    else:
        return path


def file_path(str):
    path = Path(str)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    else:
        return path


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", type=file_path)
    parser.add_argument("output_schema_path", type=file_path)
    parser.add_argument("dataset_path", type=dir_path)
    parser.add_argument("expected_result_path", type=file_path)

    args = parser.parse_args()
    result = await run_pipeline(
        config_path=args.config_path,
        output_schema_path=args.output_schema_path,
        dataset_path=args.dataset_path,
        expected_result_path=args.expected_result_path,
    )

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
