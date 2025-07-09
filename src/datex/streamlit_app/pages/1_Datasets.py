import streamlit as st
from pathlib import Path
import json
from pydantic import create_model
from typing import Dict, Any, Type, Tuple, List

type_map: Dict[str, Type[Any]] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "object": dict,
}


def parse_field(name: str, info: Dict[str, Any]) -> Tuple[str, Tuple[Any, Any]]:
    """
    Wandelt ein Schema-Feld in ein Tuple (Feldname, (Typ, Default)) um.
    """
    t = info["type"]
    if t == "array":
        item_type = type_map.get(info["items"]["type"], Any)
        py_type = List[item_type]
    else:
        py_type = type_map.get(t, Any)
    default = ...
    return name, (py_type, default)


st.set_page_config(
    page_title="Datasets",
)

data_dir = Path("./data/")
expected_result_file_name = "expected_output.json"
output_schema_file_name = "output_schema.json"

datasets = []
for item in data_dir.iterdir():
    if item.is_dir():
        datasets.append(item)

selected_dataset = st.selectbox(
    label="Choose Dataset", options=datasets, index=None, format_func=lambda x: x.name
)

if selected_dataset:
    file_tab, output_tab, expected_tab = st.columns(3)
    files = [item.name for item in selected_dataset.glob("*.pdf")]
    file_tab.write(files)

    output_schema_file_path = Path(selected_dataset / output_schema_file_name)

    with output_tab:
        st.header("Output Schema Editor")

        # Initialize session state for schema editor
        if (
            "schema_fields" not in st.session_state
            or st.session_state.get("dataset_name") != selected_dataset.name
        ):
            st.session_state.dataset_name = selected_dataset.name
            if output_schema_file_path.exists():
                with open(output_schema_file_path, "r", encoding="utf-8") as f:
                    loaded_schema = json.load(f)

                st.session_state.schema_fields = [
                    {"name": name, **props}
                    for name, props in loaded_schema.get("properties", {}).items()
                ]
            else:
                st.session_state.schema_fields = []

        # Display fields for editing
        for i, field in enumerate(st.session_state.schema_fields):
            st.markdown("---")
            key_prefix = f"field_{i}"
            cols = st.columns([3, 2, 4, 1])

            st.session_state.schema_fields[i]["name"] = cols[0].text_input(
                "Field Name", value=field.get("name", ""), key=f"{key_prefix}_name"
            )

            supported_types = [
                "string",
                "number",
                "boolean",
                "integer",
                "object",
                "array",
            ]
            current_type = field.get("type", "string")
            st.session_state.schema_fields[i]["type"] = cols[1].selectbox(
                "Type",
                options=supported_types,
                index=(
                    supported_types.index(current_type)
                    if current_type in supported_types
                    else 0
                ),
                key=f"{key_prefix}_type",
            )

            st.session_state.schema_fields[i]["description"] = cols[2].text_input(
                "Description",
                value=field.get("description", ""),
                key=f"{key_prefix}_desc",
            )

            if cols[3].button("🗑️", key=f"{key_prefix}_del"):
                st.session_state.schema_fields.pop(i)
                st.rerun()

            if st.session_state.schema_fields[i]["type"] == "array":
                items = field.get("items", {"type": "string"})
                item_types = ["string", "number", "boolean", "integer"]
                current_item_type = items.get("type", "string")
                items["type"] = st.selectbox(
                    "Array Item Type",
                    options=item_types,
                    index=(
                        item_types.index(current_item_type)
                        if current_item_type in item_types
                        else 0
                    ),
                    key=f"{key_prefix}_items_type",
                )
                st.session_state.schema_fields[i]["items"] = items

        st.markdown("---")

        col1, col2 = st.columns(2)
        if col1.button("Add Field"):
            st.session_state.schema_fields.append(
                {
                    "name": f"new_field_{len(st.session_state.schema_fields)}",
                    "type": "string",
                    "description": "",
                }
            )
            st.rerun()

        if col2.button("Save Schema"):
            new_schema_dict = {}
            valid = True
            for field in st.session_state.schema_fields:
                field_name = field.get("name")
                if not field_name:
                    st.error("Field name cannot be empty.")
                    valid = False
                    continue

                if not field_name.isidentifier():
                    st.error(
                        f"Field name '{field_name}' is not a valid Python identifier."
                    )
                    valid = False
                    continue

                field_props = field.copy()
                del field_props["name"]
                new_schema_dict[field_name] = field_props

            if valid:
                final_schema = {"type": "object", "properties": new_schema_dict}

                with open(output_schema_file_path, "w", encoding="utf-8") as f:
                    json.dump(final_schema, f, indent=2)

                st.success("Schema saved successfully!")

    # Create Pydantic model from the current state of the schema in the UI
    current_schema_props = {}
    if "schema_fields" in st.session_state:
        for field in st.session_state.get("schema_fields", []):
            field_name = field.get("name")
            if field_name:
                field_props = field.copy()
                del field_props["name"]
                current_schema_props[field_name] = field_props

    OutputModel = None
    if current_schema_props:
        fields = dict(
            parse_field(name, info) for name, info in current_schema_props.items()
        )
        OutputModel = create_model("OutputModel", **fields)  # type: ignore

    # Display expected results
    expected_result_file_path = Path(selected_dataset / expected_result_file_name)
    if expected_result_file_path.exists():
        with open(expected_result_file_path, mode="r", encoding="utf-8") as f:
            expected_result = json.load(f)
        expected_tab.write("## Expected Results")
        if OutputModel:
            for file_name, data in expected_result.items():
                with expected_tab.expander(label=str(file_name), expanded=False):
                    try:
                        st.write(OutputModel.model_validate(data))
                    except Exception as e:
                        st.error(f"Error validating {file_name}: {e}")

        else:
            expected_tab.info("No output schema defined to validate results.")
    else:
        expected_tab.write(
            f"Expected result file not found:\n{expected_result_file_path}"
        )
