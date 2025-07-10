import streamlit as st
from pathlib import Path
import json
import asyncio
from datex.extraction import run_extractions
from datex.extraction.schemas import ExtractionConfig
from datex.conversion import run_conversions

# --- Helper functions from main.py ---


def load_config(path: Path) -> ExtractionConfig:
    """Loads extraction configuration from a JSON file."""
    with open(path, "r") as file:
        config_file = json.load(file)
    return ExtractionConfig.model_validate(config_file)


def prepare_dataset(path: Path) -> list[Path]:
    """Returns a list of PDF paths in a dataset directory."""
    pdf_paths = []
    for file in path.iterdir():
        if file.suffix == ".pdf":
            pdf_paths.append(file)
    return pdf_paths


async def run_extraction_pipeline(
    config: ExtractionConfig, output_schema_path: Path, dataset_path: Path
):
    """Runs the full conversion and extraction pipeline."""
    pdf_paths = prepare_dataset(path=dataset_path)
    if not pdf_paths:
        return {}  # No files to process
    conversion_result = run_conversions(paths=pdf_paths)
    extraction_results = await run_extractions(
        output_schema_path=output_schema_path,
        config=config,
        conversion_result=conversion_result,
    )
    return extraction_results


def display_results(extraction_results, expected_results):
    """Displays extraction results, comparing with expected results if available."""
    if not extraction_results:
        st.info("The extraction returned no results.")
        return

    all_files = set(extraction_results.keys())
    if expected_results:
        all_files.update(expected_results.keys())

    for file_name in sorted(list(all_files)):
        st.subheader(f"Results for: {file_name}")

        extracted = extraction_results.get(file_name)
        expected = expected_results.get(file_name) if expected_results else None

        if expected:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### Extracted Data")
                if extracted:
                    st.json(extracted)
                else:
                    st.warning("No extraction result for this file.")
            with col2:
                st.markdown("#### Expected Data")
                st.json(expected)
        else:
            st.markdown("#### Extracted Data")
            if extracted:
                st.json(extracted)
            else:
                # This case should ideally not happen if we iterate over extracted keys
                st.warning("No extraction result for this file.")


# --- Streamlit UI ---

st.set_page_config(page_title="Run Extraction", layout="wide")
st.title("Run Extraction")

# --- Dataset Selection ---
data_dir = Path("./data/")
if not data_dir.exists():
    st.error("The `data` directory was not found. Please create it.")
    st.stop()

datasets = [item for item in data_dir.iterdir() if item.is_dir()]
dataset_names = [d.name for d in datasets]

if not datasets:
    st.warning(
        "No datasets found. Please create a dataset first on the 'Datasets' page."
    )
    st.stop()

selected_dataset_name = st.selectbox("Choose a dataset", dataset_names)

if selected_dataset_name:
    selected_dataset_path = data_dir / selected_dataset_name

    # --- Display Files in Dataset ---
    files = list(selected_dataset_path.glob("*.pdf"))
    with st.expander("Files in dataset"):
        if files:
            for f in files:
                st.write(f.name)
        else:
            st.info("No PDF files found in this dataset.")

    # --- Run Extraction Button ---
    if st.button("Run Extraction", use_container_width=True, type="primary"):
        config_path = Path("config.json")
        output_schema_path = selected_dataset_path / "output_schema.json"
        expected_result_path = selected_dataset_path / "expected_output.json"

        # --- Pre-run Checks ---
        if not config_path.exists():
            st.error(
                "`config.json` not found. Please configure the application on the 'Config' page."
            )
            st.stop()
        if not output_schema_path.exists():
            st.error(
                "`output_schema.json` not found in the dataset. Please define a schema on the 'Datasets' page."
            )
            st.stop()
        if not files:
            st.warning("No PDF files in the dataset to run extraction on.")
            st.stop()

        try:
            # --- Load Config and Expected Results ---
            config = load_config(path=config_path)
            expected_results = None
            if expected_result_path.exists():
                with open(expected_result_path, "r", encoding="utf-8") as f:
                    expected_results = json.load(f)

            # --- Execute Pipeline ---
            with st.spinner("Running extraction... This may take a while."):
                extraction_results = asyncio.run(
                    run_extraction_pipeline(
                        config=config,
                        output_schema_path=output_schema_path,
                        dataset_path=selected_dataset_path,
                    )
                )

            st.success("Extraction finished!")

            # --- Display Results ---
            st.header("Extraction Results")
            display_results(extraction_results, expected_results)

        except Exception as e:
            st.error(f"An error occurred during extraction: {e}")
            st.exception(e)  # show traceback for debugging
