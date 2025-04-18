import os
import json
from functools import lru_cache
from pathlib import Path
from utils.models.settings_model import Settings
from utils.logging import logger

CONFIG_DIR = os.path.dirname(__file__)
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')

_logger = logger.bind(module='ConfigLoader')

@lru_cache()
def get_core_config() -> Settings:
    """Load settings from the settings.json file."""
    _logger.info(f"📖 Loading settings from: {SETTINGS_FILE}")
    if not os.path.exists(SETTINGS_FILE):
        _logger.error(f"❌ Settings file not found at {SETTINGS_FILE}")
        raise RuntimeError(f"Settings file not found at {SETTINGS_FILE}. Ensure the entrypoint script has run.")
    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings_dict = json.load(f)
        settings = Settings(**settings_dict)
        _logger.success("✅ Successfully loaded settings")
        return settings
    except json.JSONDecodeError as e:
        _logger.error(f"❌ Error decoding settings file: {e}")
        raise RuntimeError(f"Error decoding settings file ({SETTINGS_FILE}): {str(e)}")
    except Exception as e:
        _logger.error(f"❌ Error loading settings: {e}")
        raise RuntimeError(f"Error loading settings from {SETTINGS_FILE}: {str(e)}")
