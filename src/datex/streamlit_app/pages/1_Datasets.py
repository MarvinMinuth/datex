import streamlit as st
from pathlib import Path
import json
from pydantic import create_model
from typing import Dict, Any, Type, Tuple, List
import copy

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
        items_info = info.get("items", {})
        item_type_str = items_info.get("type")

        if item_type_str == "object":
            properties = items_info.get("properties", {})
            nested_fields = {
                prop_name: parse_field(prop_name, prop_info)[1]
                for prop_name, prop_info in properties.items()
            }
            NestedModel = create_model(f"{name}Item", **nested_fields)  # type: ignore
            py_type = List[NestedModel]
        else:
            item_type = type_map.get(item_type_str, Any)
            py_type = List[item_type]
    else:
        py_type = type_map.get(t, Any)

    default = ...
    return name, (py_type, default)


st.set_page_config(
    page_title="Datasets",
)


# --- Helper Functions for State Management ---
def mark_schema_as_changed():
    st.session_state.schema_has_unsaved_changes = True


def add_schema_field():
    st.session_state.schema_fields.append(
        {
            "name": f"new_field_{len(st.session_state.schema_fields)}",
            "type": "string",
            "description": "",
        }
    )
    mark_schema_as_changed()


def delete_schema_field(index):
    st.session_state.schema_fields.pop(index)
    mark_schema_as_changed()


def add_object_property(field_index):
    items = st.session_state.schema_fields[field_index]["items"]
    if "properties" not in items or not isinstance(items["properties"], list):
        items["properties"] = [
            {"prop_name": k, **v} for k, v in items.get("properties", {}).items()
        ]
    items["properties"].append(
        {
            "prop_name": f"new_prop_{len(items['properties'])}",
            "type": "string",
            "description": "",
        }
    )
    mark_schema_as_changed()


def delete_object_property(field_index, prop_index):
    items = st.session_state.schema_fields[field_index]["items"]
    if "properties" in items and isinstance(items["properties"], list):
        if 0 <= prop_index < len(items["properties"]):
            items["properties"].pop(prop_index)
            mark_schema_as_changed()


def add_result_item(file_name, field_name, is_object):
    item = {} if is_object else ""
    st.session_state.expected_results[file_name][field_name].append(item)


def delete_result_item(file_name, field_name, item_index):
    st.session_state.expected_results[file_name][field_name].pop(item_index)


def create_dataset(data_dir, name):
    if name:
        new_dataset_path = data_dir / name
        if not new_dataset_path.exists():
            new_dataset_path.mkdir()
            st.sidebar.success(f"Dataset '{name}' created.")
        else:
            st.sidebar.error("Dataset with this name already exists.")
    else:
        st.sidebar.warning("Please enter a name for the new dataset.")


def delete_dataset(path):
    import shutil

    shutil.rmtree(path)
    st.success(f"Dataset '{path.name}' has been deleted.")
    # Reset selection and confirmation state
    st.session_state.confirm_delete = False
    st.session_state.selected_dataset_name = None


def delete_file(file_path):
    file_path.unlink()


def update_selected_dataset():
    st.session_state.selected_dataset_name = st.session_state.dataset_selector


def save_schema(schema_file_path):
    # Deep copy to avoid modifying the session state during the save operation
    schema_to_save = copy.deepcopy(st.session_state.schema_fields)
    new_schema_dict = {}
    valid = True
    for field in schema_to_save:
        field_name = field.get("name")
        if not field_name:
            st.error("Field name cannot be empty.")
            valid = False
            continue
        if not field_name.isidentifier():
            st.error(f"Field name '{field_name}' is not a valid identifier.")
            valid = False
            continue

        field_props = field
        del field_props["name"]

        # Convert properties list back to dict if it's an object array
        if (
            field_props.get("type") == "array"
            and field_props.get("items", {}).get("type") == "object"
        ):
            prop_list = field_props["items"].get("properties", [])
            field_props["items"]["properties"] = {
                p["prop_name"]: {
                    "type": p.get("type"),
                    "description": p.get("description"),
                }
                for p in prop_list
                if p.get("prop_name")
            }
            # Add required and additionalProperties to nested object
            field_props["items"]["required"] = list(
                field_props["items"]["properties"].keys()
            )
            field_props["items"]["additionalProperties"] = False

        new_schema_dict[field_name] = field_props

    if valid:
        final_schema = {
            "type": "object",
            "properties": new_schema_dict,
            "required": list(new_schema_dict.keys()),
            "additionalProperties": False,
        }
        with open(schema_file_path, "w", encoding="utf-8") as f:
            json.dump(final_schema, f, indent=2)
        st.success("Schema saved successfully!")
        st.session_state.saved_schema_fields = copy.deepcopy(
            st.session_state.schema_fields
        )
        st.session_state.schema_has_unsaved_changes = False


# --- Main App ---
data_dir = Path("./data/")
expected_result_file_name = "expected_output.json"
output_schema_file_name = "output_schema.json"

datasets = [item for item in data_dir.iterdir() if item.is_dir()]

# --- Sidebar ---
st.sidebar.header("Manage Datasets")
new_dataset_name = st.sidebar.text_input("New dataset name", key="new_dataset_input")
st.sidebar.button(
    "Create Dataset", on_click=create_dataset, args=(data_dir, new_dataset_name)
)

# --- Dataset Selection ---
st.header("Select Dataset")
col1, col2 = st.columns([3, 1])

# Use a key for the selectbox and manage its state
if "selected_dataset_name" not in st.session_state:
    st.session_state.selected_dataset_name = None

# Find the index of the selected dataset
current_index = None
if st.session_state.selected_dataset_name:
    try:
        current_index = [d.name for d in datasets].index(
            st.session_state.selected_dataset_name
        )
    except ValueError:
        current_index = None  # Dataset might have been deleted

col1.selectbox(
    label="Choose Dataset",
    options=[d.name for d in datasets],
    index=current_index,
    key="dataset_selector",
    label_visibility="collapsed",
    on_change=update_selected_dataset,
)
selected_dataset_name = st.session_state.selected_dataset_name
selected_dataset = data_dir / selected_dataset_name if selected_dataset_name else None


if selected_dataset:
    # --- Dataset Deletion ---
    col2.button(
        "🗑️ Delete Dataset",
        use_container_width=True,
        on_click=lambda: setattr(st.session_state, "confirm_delete", True),
    )

    if st.session_state.get("confirm_delete"):
        st.warning(
            f"Are you sure you want to delete the dataset '{selected_dataset.name}'? This action cannot be undone."
        )
        c1, c2 = st.columns(2)
        if selected_dataset:
            c1.button(
                "Yes, delete it",
                type="primary",
                on_click=delete_dataset,
                args=(selected_dataset,),
            )
        c2.button(
            "Cancel",
            on_click=lambda: setattr(st.session_state, "confirm_delete", False),
        )

    # --- Tabs ---
    if "active_tab" not in st.session_state:
        st.session_state.active_tab = "Files"

    st.session_state.active_tab = st.radio(
        "Navigation",
        ["Files", "Output Schema", "Expected Results"],
        key="tab_selector",
        horizontal=True,
        label_visibility="collapsed",
    )

    # --- File Management Tab ---
    if st.session_state.active_tab == "Files":
        st.header("Manage Files")
        uploaded_files = st.file_uploader(
            "Upload PDF files",
            type="pdf",
            accept_multiple_files=True,
            key=f"uploader_{selected_dataset.name}",
        )
        if uploaded_files:
            for uploaded_file in uploaded_files:
                with open(selected_dataset / uploaded_file.name, "wb") as f:
                    f.write(uploaded_file.getbuffer())
            st.success(f"{len(uploaded_files)} file(s) uploaded successfully.")
            # No need for manual rerun, Streamlit handles it

        st.markdown("---")
        files = list(selected_dataset.glob("*.pdf"))
        if not files:
            st.info("No PDF files in this dataset.")
        else:
            st.subheader("Existing Files")
            for f in files:
                f_col1, f_col2 = st.columns([4, 1])
                f_col1.write(f.name)
                f_col2.button(
                    "🗑️",
                    key=f"del_{f.name}",
                    use_container_width=True,
                    on_click=delete_file,
                    args=(f,),
                )

    # --- Output Schema Tab ---
    output_schema_file_path = Path(selected_dataset / output_schema_file_name)

    # --- Pydantic Model Creation ---
    current_schema_props = {}
    if "saved_schema_fields" in st.session_state:
        # Use the SAVED schema for the model
        schema_fields_for_model = copy.deepcopy(
            st.session_state.get("saved_schema_fields", [])
        )

        for field in schema_fields_for_model:
            field_name = field.get("name")
            if field_name:
                field_props = field
                del field_props["name"]
                # Ensure properties are in dict format for parsing
                if (
                    field_props.get("type") == "array"
                    and field_props.get("items", {}).get("type") == "object"
                ):
                    prop_list = field_props["items"].get("properties", [])
                    field_props["items"]["properties"] = {
                        p["prop_name"]: {
                            "type": p.get("type"),
                            "description": p.get("description"),
                        }
                        for p in prop_list
                        if p.get("prop_name")
                    }
                current_schema_props[field_name] = field_props

    OutputModel = None
    if current_schema_props:
        try:
            fields = dict(
                parse_field(name, info) for name, info in current_schema_props.items()
            )
            OutputModel = create_model("OutputModel", **fields)  # type: ignore
        except Exception as e:
            st.error(f"Error creating data model from schema: {e}")

    if st.session_state.active_tab == "Output Schema":
        st.header("Output Schema Editor")

        # Initialize or reset schema fields based on dataset selection
        if (
            "dataset_name" not in st.session_state
            or st.session_state.dataset_name != selected_dataset.name
        ):
            st.session_state.dataset_name = selected_dataset.name
            if output_schema_file_path.exists():
                with open(output_schema_file_path, "r", encoding="utf-8") as f:
                    loaded_schema = json.load(f)
                schema_fields = []
                for name, props in loaded_schema.get("properties", {}).items():
                    if (
                        props.get("type") == "array"
                        and props.get("items", {}).get("type") == "object"
                    ):
                        props["items"]["properties"] = [
                            {"prop_name": k, **v}
                            for k, v in props["items"].get("properties", {}).items()
                        ]
                    schema_fields.append({"name": name, **props})
                st.session_state.schema_fields = schema_fields
                st.session_state.saved_schema_fields = copy.deepcopy(schema_fields)
                st.session_state.schema_has_unsaved_changes = False
            else:
                st.session_state.schema_fields = []
                st.session_state.saved_schema_fields = []
                st.session_state.schema_has_unsaved_changes = False

        # Display fields for editing
        for i, field in enumerate(st.session_state.schema_fields):
            st.markdown("---")
            key_prefix = f"field_{i}"
            cols = st.columns([3, 2, 4, 1])

            field["name"] = cols[0].text_input(
                "Field Name",
                value=field.get("name", ""),
                key=f"{key_prefix}_name",
                on_change=mark_schema_as_changed,
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
            field["type"] = cols[1].selectbox(
                "Type",
                options=supported_types,
                index=supported_types.index(current_type),
                key=f"{key_prefix}_type",
                on_change=mark_schema_as_changed,
            )

            field["description"] = cols[2].text_input(
                "Description",
                value=field.get("description", ""),
                key=f"{key_prefix}_desc",
                on_change=mark_schema_as_changed,
            )

            cols[3].button(
                "🗑️", key=f"{key_prefix}_del", on_click=delete_schema_field, args=(i,)
            )

            if field["type"] == "array":
                items = field.get("items", {"type": "string"})
                item_types = ["string", "number", "boolean", "integer", "object"]
                current_item_type = items.get("type", "string")
                items["type"] = st.selectbox(
                    "Array Item Type",
                    options=item_types,
                    index=item_types.index(current_item_type),
                    key=f"{key_prefix}_items_type",
                    on_change=mark_schema_as_changed,
                )

                if items["type"] == "object":
                    if "properties" not in items or not isinstance(
                        items["properties"], list
                    ):
                        items["properties"] = [
                            {"prop_name": k, **v}
                            for k, v in items.get("properties", {}).items()
                        ]

                    with st.container():
                        st.markdown("###### Object Properties")
                        for j, prop in enumerate(items["properties"]):
                            prop_key = f"{key_prefix}_prop_{j}"
                            p_cols = st.columns([3, 2, 4, 1])
                            prop["prop_name"] = p_cols[0].text_input(
                                "Prop. Name",
                                value=prop.get("prop_name", ""),
                                key=f"{prop_key}_name",
                                on_change=mark_schema_as_changed,
                            )
                            prop_types = ["string", "number", "boolean", "integer"]
                            prop["type"] = p_cols[1].selectbox(
                                "Prop. Type",
                                options=prop_types,
                                index=prop_types.index(prop.get("type", "string")),
                                key=f"{prop_key}_type",
                                on_change=mark_schema_as_changed,
                            )
                            prop["description"] = p_cols[2].text_input(
                                "Prop. Desc.",
                                value=prop.get("description", ""),
                                key=f"{prop_key}_desc",
                                on_change=mark_schema_as_changed,
                            )
                            p_cols[3].button(
                                "🗑️",
                                key=f"{prop_key}_del",
                                on_click=delete_object_property,
                                args=(i, j),
                            )

                        st.button(
                            "Add Property",
                            key=f"{key_prefix}_add_prop",
                            on_click=add_object_property,
                            args=(i,),
                        )

                field["items"] = items

        st.markdown("---")
        # Display unsaved changes warning
        if st.session_state.get("schema_has_unsaved_changes", False):
            st.warning("You have unsaved changes in the schema.")

        st.button("Add Field", on_click=add_schema_field)
        st.button("Save Schema", on_click=save_schema, args=(output_schema_file_path,))

    # --- Expected Results Tab ---
    elif st.session_state.active_tab == "Expected Results":
        st.header("Expected Results Editor")
        expected_result_file_path = Path(selected_dataset / expected_result_file_name)

        if (
            "expected_results" not in st.session_state
            or st.session_state.dataset_name != selected_dataset.name
        ):
            if expected_result_file_path.exists():
                with open(expected_result_file_path, "r", encoding="utf-8") as f:
                    st.session_state.expected_results = json.load(f)
            else:
                st.session_state.expected_results = {}

        if not OutputModel:
            st.info("Define a valid output schema to create expected results.")
        else:
            pdf_files = [item.name for item in selected_dataset.glob("*.pdf")]
            for file_name in pdf_files:
                if file_name not in st.session_state.expected_results:
                    st.session_state.expected_results[file_name] = {}

                with st.expander(f"Edit results for: {file_name}", expanded=False):
                    current_data = st.session_state.expected_results[file_name]
                    for field_name, field_info in OutputModel.model_fields.items():
                        field_type = field_info.annotation
                        label = f"{field_name}"
                        key_prefix = f"{file_name}_{field_name}"

                        if (
                            hasattr(field_type, "__origin__")
                            and field_type.__origin__ is list
                        ):
                            st.markdown(f"**{label}** (list of items)")
                            if field_name not in current_data or not isinstance(
                                current_data[field_name], list
                            ):
                                current_data[field_name] = []

                            item_model = field_type.__args__[0]
                            is_object_list = hasattr(item_model, "model_fields")

                            for item_idx, item_data in enumerate(
                                current_data[field_name]
                            ):
                                st.markdown("---")
                                item_key_prefix = f"{key_prefix}_item_{item_idx}"
                                if is_object_list:
                                    for (
                                        prop_name,
                                        prop_info,
                                    ) in item_model.model_fields.items():
                                        prop_type = prop_info.annotation
                                        prop_label = f"{prop_name}"
                                        prop_key = f"{item_key_prefix}_{prop_name}"
                                        if prop_type is str:
                                            item_data[prop_name] = st.text_input(
                                                prop_label,
                                                value=item_data.get(prop_name, ""),
                                                key=f"{prop_key}_str",
                                            )
                                        elif prop_type is int:
                                            item_data[prop_name] = st.number_input(
                                                prop_label,
                                                value=item_data.get(prop_name, 0),
                                                step=1,
                                                key=f"{prop_key}_int",
                                            )
                                        elif prop_type is float:
                                            item_data[prop_name] = st.number_input(
                                                prop_label,
                                                value=item_data.get(prop_name, 0.0),
                                                key=f"{prop_key}_float",
                                            )
                                        elif prop_type is bool:
                                            item_data[prop_name] = st.checkbox(
                                                prop_label,
                                                value=item_data.get(prop_name, False),
                                                key=f"{prop_key}_bool",
                                            )
                                else:
                                    current_data[field_name][item_idx] = st.text_input(
                                        f"Item {item_idx+1}",
                                        value=item_data,
                                        key=f"{item_key_prefix}_simple",
                                    )

                                st.button(
                                    f"Remove Item {item_idx+1}",
                                    key=f"{item_key_prefix}_del",
                                    on_click=delete_result_item,
                                    args=(file_name, field_name, item_idx),
                                )

                            st.button(
                                f"Add Item to {field_name}",
                                key=f"{key_prefix}_add",
                                on_click=add_result_item,
                                args=(file_name, field_name, is_object_list),
                            )

                        else:  # Handle simple types
                            if field_type is str:
                                current_data[field_name] = st.text_input(
                                    label,
                                    value=current_data.get(field_name, ""),
                                    key=f"{key_prefix}_str",
                                )
                            elif field_type is int:
                                current_data[field_name] = st.number_input(
                                    label,
                                    value=current_data.get(field_name, 0),
                                    step=1,
                                    key=f"{key_prefix}_int",
                                )
                            elif field_type is float:
                                current_data[field_name] = st.number_input(
                                    label,
                                    value=current_data.get(field_name, 0.0),
                                    key=f"{key_prefix}_float",
                                )
                            elif field_type is bool:
                                current_data[field_name] = st.checkbox(
                                    label,
                                    value=current_data.get(field_name, False),
                                    key=f"{key_prefix}_bool",
                                )

            st.markdown("---")
            if st.button("Save Expected Results"):
                with open(expected_result_file_path, "w", encoding="utf-8") as f:
                    json.dump(st.session_state.expected_results, f, indent=2)
                st.success("Expected results saved successfully!")
