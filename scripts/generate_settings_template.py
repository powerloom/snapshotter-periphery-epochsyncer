import json
import os

def generate_template():
    """Generate template settings.json with placeholder values"""
    template = {
        "source_rpc": {
            "full_nodes": [
                {
                    "url": "${SOURCE_RPC_URL}",
                    "rate_limit": {
                        "requests_per_second": "${SOURCE_RPC_RATE_LIMIT}"
                    }
                }
            ],
            "archive_nodes": None,
            "force_archive_blocks": None,
            "retry": "${SOURCE_RPC_MAX_RETRIES}",
            "request_time_out": "${SOURCE_RPC_TIMEOUT}",
            "connection_limits": {
                "max_connections": 100,
                "max_keepalive_connections": 50,
                "keepalive_expiry": 300
            },
            "polling_interval": "${SOURCE_RPC_POLLING_INTERVAL}",
            "semaphore_value": 20
        },
        "powerloom_rpc": {
            "full_nodes": [
                {
                    "url": "${POWERLOOM_RPC_URL}",
                    "rate_limit": {
                        "requests_per_second": "${POWERLOOM_RPC_RATE_LIMIT}"
                    }
                }
            ],
            "archive_nodes": None,
            "force_archive_blocks": None,
            "retry": "${POWERLOOM_RPC_RETRY}",
            "request_time_out": "${POWERLOOM_RPC_TIMEOUT}",
            "connection_limits": {
                "max_connections": 100,
                "max_keepalive_connections": 50,
                "keepalive_expiry": 300
            },
            "polling_interval": "${POWERLOOM_RPC_POLLING_INTERVAL}",
            "semaphore_value": 20
        },
        "logs": {
            "write_to_files": "${WRITE_LOGS_TO_FILES}"
        },
        "redis": {
            "host": "${REDIS_HOST}",
            "port": "${REDIS_PORT}",
            "db": "${REDIS_DB}",
            "password": "${REDIS_PASSWORD}",
            "ssl": "${REDIS_SSL}",
            "cluster_mode": "${REDIS_CLUSTER_MODE}"
        },
        "protocol_state_contract_address": "${PROTOCOL_STATE_CONTRACT_ADDRESS}",
        "data_market_contract_address": "${DATA_MARKET_CONTRACT_ADDRESS}",
        "namespace": "${NAMESPACE}",
        "instance_id": "${INSTANCE_ID}"
    }

    # Ensure config directory exists
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
    os.makedirs(config_dir, exist_ok=True)
    template_path = os.path.join(config_dir, 'settings.template.json')

    with open(template_path, 'w') as f:
        json.dump(template, f, indent=2)
    print(f"Generated template at {template_path}")

if __name__ == "__main__":
    generate_template()