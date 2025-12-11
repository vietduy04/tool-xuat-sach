"""Session state persistence for configuration."""

import json
import os
from typing import Any, Dict

from utils.logger import get_logger

logger = get_logger()


def load_config() -> Dict[str, Any]:
    """Load persisted configuration from JSON file."""
    config_path = ".streamlit_config.json"  # Change config path
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load config: {e}. Using defaults.")

    # Return default configuration
    return {
        "lookup": None,
        "rule_rd": None,
        "rule_kn": None,
        # "output_folder": DEFAULT_OUTPUT_FOLDER,
        # "output_format": DEFAULT_OUTPUT_FORMAT,
    }


def save_config(config_data: Dict[str, Any]) -> None:
    """Save configuration to JSON file."""
    config_path = "config.json"  # Change config path
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Configuration saved to {config_path}")
    except Exception as e:
        logger.error(f"Failed to save config: {e}")


def update_config(key: str, value: Any) -> None:
    """Update a single configuration value."""
    config_data = load_config()
    config_data[key] = value
    save_config(config_data)
