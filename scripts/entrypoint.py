#!/usr/bin/env python3
import os
import json
import subprocess
from string import Template
from dotenv import load_dotenv

CONFIG_DIR = 'config'  # Relative to WORKDIR (/app)
TEMPLATE_FILE = os.path.join(CONFIG_DIR, 'settings.template.json')
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')

def fill_template():
    """Fill settings template with environment variables"""
    load_dotenv()  # Load .env file if present

    if not os.path.exists(TEMPLATE_FILE):
        print(f"ERROR: Template file not found at {TEMPLATE_FILE}")
        exit(1)

    with open(TEMPLATE_FILE, 'r') as f:
        template = Template(f.read())

    print("--- Substituting settings template ---")
    try:
        filled_str = template.substitute(os.environ)
        # Validate if it's valid JSON before writing
        json.loads(filled_str)
    except KeyError as e:
        print(f"ERROR: Missing environment variable for substitution: {e}. Check template and env vars.")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Substituted template resulted in invalid JSON: {e}")
        print("--- Substituted Content ---")
        print(filled_str)
        print("--------------------------")
        exit(1)
    except Exception as e:
        print(f"ERROR: Failed during template substitution: {e}")
        exit(1)

    print(f"Writing final settings to {SETTINGS_FILE}")
    with open(SETTINGS_FILE, 'w') as f:
        f.write(filled_str)
    print("--- Settings substitution complete ---")

if __name__ == "__main__":
    fill_template()
    print("Executing event detector: python event_detector.py")
    result = subprocess.run(["python", "event_detector.py"], check=False)
    exit(result.returncode)
