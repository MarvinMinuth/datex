import streamlit as st
import json
from pathlib import Path
from datex.extraction.schemas import Provider

# Define the path to the config file
CONFIG_FILE = Path("config.json")


# Function to load the config
def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


# Function to save the config
def save_config(config_data):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_data, f, indent=4)


# Page title
st.title("Configuration Editor")

# Load current config
config = load_config()

# Display and edit config values
st.header("Model Configuration")

# Get provider options from the enum
provider_options = [provider.value for provider in Provider]
current_provider = config.get("provider", provider_options[0])
provider_index = (
    provider_options.index(current_provider)
    if current_provider in provider_options
    else 0
)


# Use a selectbox for the provider
config["provider"] = st.selectbox(
    "Provider", options=provider_options, index=provider_index
)
config["model_name"] = st.text_input("Model Name", config.get("model_name", ""))
config["temperature"] = st.number_input(
    "Temperature",
    value=config.get("temperature", 0.1),
    min_value=0.0,
    max_value=1.0,
    step=0.01,
)
config["top_p"] = st.number_input(
    "Top P", value=config.get("top_p", 0.1), min_value=0.0, max_value=1.0, step=0.01
)

st.header("Prompts")
config["system_prompt"] = st.text_area(
    "System Prompt", config.get("system_prompt", ""), height=150
)
config["user_prompt"] = st.text_area(
    "User Prompt", config.get("user_prompt", ""), height=150
)

# Save button
if st.button("Save Configuration"):
    save_config(config)
    st.success("Configuration saved successfully!")
    st.rerun()
