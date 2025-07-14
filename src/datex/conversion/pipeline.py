from datex.conversion.schemas import ConversionTask, ConversionResult
from datetime import datetime


def run_conversions(task: ConversionTask) -> ConversionResult:
    """
    Runs the conversion process based on the strategy defined in the task.

    Args:
        task: A ConversionTask object containing file paths and the conversion strategy.

    Returns:
        A ConversionResult object with the outcome of the conversion.
    """
    print(f"Converting files using strategy: {task.strategy.name}...")

    # Instantiate the strategy class directly from the enum member
    strategy_instance = task.strategy.strategy_class(task)

    # Execute the conversion by calling the strategy instance
    result = strategy_instance()

    print("Conversion finished.")
    return result
