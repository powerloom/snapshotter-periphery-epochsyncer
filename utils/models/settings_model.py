from pydantic import BaseModel
from typing import List, Optional, Union

class RateLimitConfig(BaseModel):
    """RPC Rate limit configuration model."""
    requests_per_second: int

class RPCNodeConfig(BaseModel):
    """RPC node configuration model."""
    url: str
    rate_limit: RateLimitConfig

class ConnectionLimits(BaseModel):
    """Connection limits configuration model."""
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300

class RPCConfig(BaseModel):
    """RPC configuration model."""
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits
    polling_interval: float
    semaphore_value: int = 20

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
    source_rpc: RPCConfig
    powerloom_rpc: RPCConfig
    logs: Logs
    redis: Redis
    protocol_state_contract_address: str
    data_market_contract_address: str
    namespace: str
    instance_id: str