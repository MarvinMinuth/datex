import streamlit as st
from pathlib import Path
import json
import asyncio
import shutil
from datetime import datetime
from datex.extraction import run_extractions
from datex.extraction.schemas import ExtractionConfig
from datex.conversion import run_conversions

# --- Helper functions ---


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


def get_schema_fields(output_schema_path: Path) -> dict:
    """Loads the output schema and returns its properties."""
    if not output_schema_path.exists():
        return {}
    with open(output_schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return schema.get("properties", {})


def display_comparison_table(extracted_data, expected_data, schema_fields):
    """Displays a side-by-side comparison table for a single file."""
    st.markdown(
        """
    <style>
    .stTable > table {
        table-layout: auto;
    }
    .stTable th, .stTable td {
        vertical-align: top;
        border: 1px solid #444;
        padding: 8px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    header_cols = st.columns([1, 2, 2])
    header_cols[0].markdown("**Field**")
    header_cols[1].markdown("**Extracted Value**")
    header_cols[2].markdown("**Expected Value**")
    st.markdown("---")

    for field, props in schema_fields.items():
        row_cols = st.columns([1, 2, 2])
        row_cols[0].markdown(f"**{field}**")

        extracted_value = extracted_data.get(field)
        expected_value = expected_data.get(field)

        # Handle list comparisons
        if props.get("type") == "array":
            with row_cols[1]:
                st.markdown("*List of items*")
                if isinstance(extracted_value, list):
                    for i, item in enumerate(extracted_value):
                        st.markdown(f"**Item {i+1}**")
                        if isinstance(item, (dict, list)):
                            st.json(item)
                        else:
                            st.markdown(f"```{item}```")
                        if i < len(extracted_value) - 1:
                            st.markdown("---")
                elif extracted_value is None:
                    st.info("Not extracted.")
                else:
                    st.warning(
                        f"Expected a list, but got: `{type(extracted_value).__name__}`"
                    )
                    st.json(extracted_value, expanded=True)

            with row_cols[2]:
                st.markdown("*List of items*")
                if isinstance(expected_value, list):
                    for i, item in enumerate(expected_value):
                        st.markdown(f"**Item {i+1}**")
                        if isinstance(item, (dict, list)):
                            st.json(item)
                        else:
                            st.markdown(f"```{item}```")
                        if i < len(expected_value) - 1:
                            st.markdown("---")
                elif expected_value is None:
                    st.info("Not expected.")
                else:
                    st.warning(
                        f"Expected a list, but got: `{type(expected_value).__name__}`"
                    )
                    st.json(expected_value, expanded=True)
        else:
            # Handle simple types
            extracted_display = (
                json.dumps(extracted_value, indent=2)
                if isinstance(extracted_value, (dict))
                else extracted_value
            )
            expected_display = (
                json.dumps(expected_value, indent=2)
                if isinstance(expected_value, (dict))
                else expected_value
            )

            row_cols[1].markdown(
                f"```\n{extracted_display}\n```"
                if extracted_value is not None
                else "N/A"
            )
            row_cols[2].markdown(
                f"```\n{expected_display}\n```" if expected_value is not None else "N/A"
            )

        st.markdown("---")


def display_results(extraction_results, expected_results, schema_fields):
    """Displays extraction results, comparing with expected results if available."""
    if not extraction_results and not expected_results:
        st.info("There are no results to display.")
        return

    all_files = set(extraction_results.keys()) | set(
        expected_results.keys() if expected_results else []
    )

    for file_name in sorted(list(all_files)):
        with st.expander(f"Comparison for: {file_name}", expanded=True):
            extracted = extraction_results.get(file_name, {})
            expected = expected_results.get(file_name, {}) if expected_results else {}

            if expected:
                display_comparison_table(extracted, expected, schema_fields)
            else:
                st.markdown("#### Extracted Data (no expected output for comparison)")
                st.json(extracted)


def save_run_results(run_folder_path: Path, config_path: Path, results: dict):
    """Saves the extraction results and config to a new folder."""
    try:
        run_folder_path.mkdir(parents=True, exist_ok=True)
        # Save the results
        with open(
            run_folder_path / "extraction_result.json", "w", encoding="utf-8"
        ) as f:
            json.dump(results, f, indent=2)
        # Copy the config file
        shutil.copy(config_path, run_folder_path / "config.json")
        st.success(f"Run saved successfully to: `{run_folder_path}`")
    except Exception as e:
        st.error(f"Failed to save run: {e}")


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

            # Store results in session state to allow saving
            st.session_state.latest_run_results = extraction_results
            st.session_state.latest_run_dataset_path = selected_dataset_path
            st.session_state.latest_run_config_path = config_path
            st.session_state.run_saved = False  # Reset save state for new run

            # --- Display Results ---
            st.header("Extraction Results")
            schema_fields = get_schema_fields(output_schema_path)
            if not schema_fields:
                st.warning("Could not load schema fields. Displaying raw JSON.")
                st.json(extraction_results)
            else:
                display_results(extraction_results, expected_results, schema_fields)

        except Exception as e:
            st.error(f"An error occurred during extraction: {e}")
            st.exception(e)  # show traceback for debugging

    # --- Save Run Section ---
    if (
        "latest_run_results" in st.session_state
        and st.session_state.latest_run_results
        and not st.session_state.get("run_saved", False)
    ):
        st.markdown("---")
        if st.button("Save Current Run"):
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            run_folder_path = (
                st.session_state.latest_run_dataset_path / "runs" / timestamp
            )

            save_run_results(
                run_folder_path=run_folder_path,
                config_path=st.session_state.latest_run_config_path,
                results=st.session_state.latest_run_results,
            )
            st.session_state.run_saved = True
            st.rerun()
