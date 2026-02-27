import os

import redis.asyncio as aioredis
from config.loader import get_core_config
from utils.logging import logger
import asyncio
from typing import Optional


def _default_redis_pool_size() -> int:
    """Pool size from REDIS_POOL_SIZE env var if set, else 10."""
    return int(os.environ.get('REDIS_POOL_SIZE', '10'))


class RedisPool:
    _pool: Optional[aioredis.Redis] = None
    _lock = asyncio.Lock()

    def __init__(self):
        # Private constructor to prevent direct instantiation
        self.settings = get_core_config()
        self._logger = logger.bind(module='RedisPool')

    @classmethod
    async def get_pool(cls) -> aioredis.Redis:
        """Get or create Redis connection pool."""
        if cls._pool is None:
            async with cls._lock:
                # Double-check locking
                if cls._pool is None:
                    instance = cls()
                    redis_settings = instance.settings.redis
                    redis_url = f"redis{'s' if redis_settings.ssl else ''}://{':' + redis_settings.password + '@' if redis_settings.password else ''}{redis_settings.host}:{redis_settings.port}/{redis_settings.db}"
                    try:
                        pool_size = _default_redis_pool_size()
                        instance._logger.info(
                            f"Creating Redis connection pool for {redis_settings.host}:{redis_settings.port}/{redis_settings.db} "
                            f"(pool_size={pool_size})"
                        )
                        cls._pool = await aioredis.from_url(
                            redis_url,
                            encoding="utf-8",
                            decode_responses=True,
                            max_connections=pool_size,
                        )
                        # Test connection
                        await cls._pool.ping()
                        instance._logger.success("✅ Successfully connected to Redis.")
                    except Exception as e:
                        instance._logger.error(f"💥 Failed to connect to Redis at {redis_url}: {e}")
                        raise ConnectionError(f"Failed to initialize Redis pool: {e}") from e
        return cls._pool

    @classmethod
    async def close(cls):
        """Close the Redis connection pool."""
        if cls._pool:
            logger.info("Closing Redis connection pool...")
            await cls._pool.close()
            # await cls._pool.connection_pool.disconnect() # For older redis versions
            cls._pool = None
            logger.info("Redis connection pool closed.")