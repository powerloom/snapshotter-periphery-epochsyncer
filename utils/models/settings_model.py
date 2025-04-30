from pydantic import BaseModel
from rpc_helper.utils.models.settings_model import RPCConfigFull
from typing import List, Optional, Union

class RateLimitConfig(BaseModel):
    """RPC Rate limit configuration model."""
    requests_per_second: int

class Logs(BaseModel):
    """Logging configuration model."""
    write_to_files: bool = True

class Redis(BaseModel):
    """Redis configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False

class Settings(BaseModel):
    """Main settings configuration model."""
    source_rpc: RPCConfigFull
    powerloom_rpc: RPCConfigFull
    logs: Logs
    redis: Redis
    protocol_state_contract_address: str
    data_market_contract_address: str
    namespace: str
    instance_id: str