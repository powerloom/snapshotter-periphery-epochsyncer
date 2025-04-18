def block_tx_htable_key(namespace: str, block_number: int) -> str:
    return f'block_txs:{block_number}:{namespace}'


def block_cache_key(namespace: str) -> str:
    """Key for sorted set storing cached block details."""
    return f'block_cache:{namespace}'

