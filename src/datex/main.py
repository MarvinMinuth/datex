from dotenv import load_dotenv
from pathlib import Path
import json
from datex.extraction import run_extractions
from datex.extraction.schemas import ExtractionConfig
from datex.conversion import run_conversions
import asyncio
import argparse

load_dotenv()


def load_config(path: Path) -> ExtractionConfig:
    with open(path, "r") as file:
        config_file = json.load(file)

    config = ExtractionConfig.model_validate(config_file)

    return config


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


async def run_pipeline(config_path: Path, output_schema_path: Path, dataset_path: Path):
    config = load_config(path=config_path)
    pdf_paths, expected_output = prepare_dataset(dataset_path)

    conversion_result = run_conversions(paths=pdf_paths)
    extraction_results = await run_extractions(
        output_schema_path=output_schema_path,
        config=config,
        conversion_result=conversion_result,
    )

    return (extraction_results, expected_output)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path")
    parser.add_argument("output_schema_path")
    parser.add_argument("dataset_path")

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
