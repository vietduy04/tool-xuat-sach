"""Session state persistence for configuration."""

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable

# from utils.logger import get_logger

# logger = get_logger()

def get_config_path() -> Path:
    """
    Get the path to the config file in LocalAppData
    
    Returns:
        Path to config.json in %LOCALAPPDATA%/ETLTool/
    """
    local_app_data = os.getenv('LOCALAPPDATA', os.path.expanduser('~'))
    config_dir = Path(local_app_data) / 'VTP_Streamline_ETL'
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / 'config.json'


def load_config() -> Dict[str, Any]:
    """Load persisted configuration from JSON file."""
    config_path = get_config_path()
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            # logger.warning(f"Failed to load config: {e}. Using defaults.")
            pass
    # Return default configuration
    return {
        "common": {
            "thamchieu_noitinh": ""
        },
        "xuat_sach_hub": {
            "rule_rd_folder": "",
            "rule_rd_file": "",
            "rule_kn_folder": "",
            "rule_kn_file": "",
            "output_rd_folder": "",
            "output_kn_folder": ""
        },
        "xuat_sach_ttkt": {
            "rule_folder": "",
            "rule_file": "",
            "output_folder": ""
        },
        "pipeline_options": {
            "pipeline_select": "",
            "fast_mode": "False"
        }
    }

def _get_nested_value(data: dict, keys: list[str], default=""):
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current

def _set_nested_value(data: Dict[str, Any], keys: Iterable[str], value: Any) -> None:
    """Set a value in a nested dict, creating intermediate dicts if needed."""
    current = data
    *parents, last_key = keys

    for key in parents:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]

    current[last_key] = value

def save_config(config_data: Dict[str, Any]) -> None:
    """Save configuration to JSON file."""
    config_path = get_config_path()
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
        # logger.info(f"Configuration saved to {config_path}")
    except Exception as e:
        # logger.error(f"Failed to save config: {e}")
        raise


def update_config(keys: list[str], value: Any) -> None:
    """
    Update a nested configuration value.
    """
    config_data = load_config()
    _set_nested_value(config_data, keys, value)
    save_config(config_data)