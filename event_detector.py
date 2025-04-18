import asyncio
import json
from utils.logging import logger, configure_file_logging
from config.loader import get_core_config
from web3 import Web3
from redis import asyncio as aioredis
from utils.rpc import RpcHelper, get_event_sig_and_abi
from utils.redis.redis_conn import RedisPool
from utils.redis.redis_keys import block_cache_key

class EpochEventDetector:
    _redis: aioredis.Redis
    def __init__(self):
        self.settings = get_core_config()
        self._powerloom_rpc_helper = RpcHelper(self.settings.powerloom_rpc)
        self.logger = logger.bind(module='EpochEventDetector')
        self._last_processed_block = None
        with open('utils/abi/ProtocolContract.json', 'r') as f:
            self.protocol_state_abi = json.load(f)

    async def init(self):
        """Initialize RPC connection"""
        await self._powerloom_rpc_helper.init()
        # Set up the contract and event ABIs
        self.contract = self._powerloom_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.to_checksum_address(self.settings.protocol_state_contract_address),
            abi=self.protocol_state_abi,
        )

        # Define event ABIs and signatures for monitoring
        EVENTS_ABI = {
            'EpochReleased': self.contract.events.EpochReleased._get_event_abi(),
        }

        EVENT_SIGS = {
            'EpochReleased': 'EpochReleased(address,uint256,uint256,uint256,uint256)',
        }

        self.event_sig, self.event_abi = get_event_sig_and_abi(
            EVENT_SIGS,
            EVENTS_ABI,
        )
        self._redis = await RedisPool.get_pool()
        self.logger.info("✅ Successfully connected to Redis.")

    async def get_events(self, from_block: int, to_block: int):
        """Get EpochReleased events in block range"""
        self.logger.info("Getting events from block {} to block {}", from_block, to_block)
        events = await self._powerloom_rpc_helper.get_events_logs(
            contract_address=self.contract.address,
            to_block=to_block,
            from_block=from_block,
            topics=[self.event_sig],
            event_abi=self.event_abi
        )
        
        for event in events:
            self.logger.info(
                "Epoch Released - DataMarket: {}, EpochId: {}, Begin: {}, End: {}, Timestamp: {}",
                event.args.dataMarketAddress,
                event.args.epochId,
                event.args.begin,
                event.args.end,
                event.args.timestamp
            )
            
            # check the redis cache whether block details are already present
            # TODO: launch a background task to check for availability of blocks in redis
            cached_blocks_zset = block_cache_key(self.settings.namespace)
            block_details = await self._redis.zrangebyscore(
                name=cached_blocks_zset, 
                min=event.args.begin, 
                max=event.args.end, 
                withscores=True
            )
            
            value_scores = {int(score) for _, score in block_details}
            # check if all blocks are present in the redis cache
            if all(block_num in value_scores for block_num in range(event.args.begin, event.args.end + 1)):
                self.logger.info("✅ Block cache found in Redis in range {} to {}", event.args.begin, event.args.end)
            else:
                self.logger.info("❌ Block cache not found in Redis in range {} to {}", event.args.begin, event.args.end)
            
        return events

    async def detect_events(self):
        first_run = True
        """Main event detection loop"""
        try:
            while True:
                try:
                    current_block = await self._powerloom_rpc_helper.get_current_block_number()
                    
                    if not self._last_processed_block:
                        self._last_processed_block = current_block - 1
                        self.logger.info("Starting to listen from block {}", self._last_processed_block)
                    
                    if current_block > self._last_processed_block:
                        # Get events from last processed to current
                        await self.get_events(
                            from_block=self._last_processed_block + 1 if not first_run else self._last_processed_block,
                            to_block=current_block
                        )
                        self._last_processed_block = current_block
                    
                    # Wait before next check
                    first_run = False
                    await asyncio.sleep(self.settings.powerloom_rpc.polling_interval)

                except Exception as e:
                    self.logger.opt(exception=True).error("Error processing events: {}", str(e))
                    await asyncio.sleep(5)  # Wait before retrying
                    raise e

        except Exception as e:
            self.logger.error("Fatal error in event listener: {}", str(e))
            raise


async def main():
    # Configure logging
    configure_file_logging()
    
    # Create and start detector
    detector = EpochEventDetector()
    await detector.init()
    await detector.detect_events()

if __name__ == "__main__":
    asyncio.run(main())
