from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from datex.conversion.image_per_page import convert


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
