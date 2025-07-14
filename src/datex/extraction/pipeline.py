from datex.extraction.schemas import ExtractionTask, ExtractionResult, ExtractedFile
from datex.extraction.strategies import ExtractionStrategy
import json
import asyncio
from datetime import datetime


async def run_extractions(task: ExtractionTask) -> ExtractionResult:
    """
    Runs the extraction process based on the strategy and data defined in the task.

    Args:
        task: An ExtractionTask object containing the config, schema, and converted files.

    Returns:
        An ExtractionResult object with the outcome of the extraction.
    """
    print(f"Extracting data using provider: {task.config.provider.value}...")
    start_time = datetime.now()

    try:
        strategy_enum = ExtractionStrategy(task.config.provider)
        extractor = strategy_enum.strategy_class(
            config=task.config, output_schema=task.output_schema
        )
    except ValueError:
        raise ValueError(f"Provider {task.config.provider} not supported.")

    async def extract_file(file_to_extract):
        try:
            result_str = await extractor(input_data=file_to_extract.parts)
            return ExtractedFile(
                file_path=file_to_extract.file_path, data=json.loads(result_str)
            )
        except json.JSONDecodeError as e:
            error_message = f"Error decoding JSON: {e}"
            print(f"{file_to_extract.file_path}: {error_message}")
            return ExtractedFile(
                file_path=file_to_extract.file_path, error=error_message
            )
        except Exception as e:
            error_message = f"An error occurred during extraction: {e}"
            print(f"{file_to_extract.file_path}: {error_message}")
            return ExtractedFile(
                file_path=file_to_extract.file_path, error=error_message
            )

    extraction_coroutines = [extract_file(f) for f in task.files]
    extracted_files = await asyncio.gather(*extraction_coroutines)

    end_time = datetime.now()
    duration = int((end_time - start_time).total_seconds())

    return ExtractionResult(
        status="success",
        duration=duration,
        files=extracted_files,
    )
