import asyncio
import json
import dramatiq
from web3 import Web3
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware.asyncio import AsyncIO
from redis import asyncio as aioredis
from utils.logging import logger, configure_file_logging
from config.loader import get_core_config
from rpc_helper.rpc import RpcHelper, get_event_sig_and_abi
from utils.redis.redis_conn import RedisPool
from utils.redis.redis_keys import block_cache_key, block_tx_htable_key, event_detector_last_processed_block
from utils.models.message_models import EpochReleasedEvent

class EpochEventDetector:
    _redis: aioredis.Redis
    _broker: RedisBroker
    _cache_check_tasks: dict[str, asyncio.Task]
    _last_processed_block: int
    def __init__(self):
        self.settings = get_core_config()
        self._powerloom_rpc_helper = RpcHelper(self.settings.powerloom_rpc)
        self.logger = logger.bind(module='EpochEventDetector')
        self._cache_check_tasks = {}
        with open('utils/abi/ProtocolContract.json', 'r') as f:
            self.protocol_state_abi = json.load(f)
        self._event_detection_q = f'powerloom-event-detector_{self.settings.namespace}_{self.settings.instance_id}'
        

    async def init(self):
        self._last_processed_block = 0
        """Initialize RPC connection"""
        await self._powerloom_rpc_helper.init()
        # Set up the contract and event ABIs
        self.contract = self._powerloom_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.to_checksum_address(self.settings.protocol_state_contract_address),
            abi=self.protocol_state_abi,
        )
        self.data_market_address = Web3.to_checksum_address(self.settings.data_market_contract_address)

        EVENTS_ABI = {
            'EpochReleased': self.contract.events.EpochReleased._get_event_abi(),
            'DayStartedEvent': self.contract.events.DayStartedEvent._get_event_abi(),
        }

        EVENT_SIGS = {
            'EpochReleased': 'EpochReleased(address,uint256,uint256,uint256,uint256)',
            'DayStartedEvent': 'DayStartedEvent(address,uint256,uint256)',
        }

        self.event_sig, self.event_abi = get_event_sig_and_abi(
            EVENT_SIGS,
            EVENTS_ABI,
        )
        self._redis = await RedisPool.get_pool()
        # Configure Redis broker with minimal middleware
        self._broker = RedisBroker(
            host=self.settings.redis.host,
            port=self.settings.redis.port,
            password=self.settings.redis.password,
            db=self.settings.redis.db,
        )
        self._broker.add_middleware(AsyncIO())
        
        # Remove Prometheus middleware to avoid errors
        middleware = self._broker.middleware[:]  # Make a copy
        for m in middleware:
            if m.__class__.__name__ == 'Prometheus':
                self._broker.middleware.remove(m)
        
        dramatiq.set_broker(self._broker)

    async def _check_cache_and_send(self, event, polling_interval: int = 1):
        """
        Background task to check cache availability and send message when ready
        """
        task_id = f"{event.args.epochId}_{event.args.begin}_{event.args.end}"
        blocks_cached = False
        block_txs_cached = False
        # map block number to list of tx hashes
        expected_block_txs_cache = dict()
        # boolean map of block number to status whether all tx hashes are cached
        expected_txs_cached_status = {block_num: False for block_num in range(event.args.begin, event.args.end + 1)}
        count = 0
        try:
            while True:
                if not blocks_cached:
                    cached_blocks_zset = block_cache_key(self.settings.namespace)
                    block_details = await self._redis.zrangebyscore(
                        name=cached_blocks_zset, 
                        min=event.args.begin, 
                        max=event.args.end, 
                        withscores=True,
                        score_cast_func=int
                    )
                    
                    value_scores = {x[1] for x in block_details}
                    for block_deets, block_num in block_details:
                        expected_block_txs_cache.update({int(block_num): json.loads(block_deets)['transactions']})
                    if all(block_num in value_scores for block_num in range(event.args.begin, event.args.end + 1)):
                        self.logger.info("✅ Block cache found in Redis in range {} to {}", event.args.begin, event.args.end)
                        blocks_cached = True
                else:
                    self.logger.info("Waiting for blocks in range {} to {} to be cached", event.args.begin, event.args.end)
                if not block_txs_cached:
                    for block_num in range(event.args.begin, event.args.end + 1):
                        if block_num in expected_block_txs_cache:
                            block_htable_txs_cached = await self._redis.hkeys(block_tx_htable_key(self.settings.namespace, block_num))
                            self.logger.info('Checking {} keys in block {} txs htable cache against expected {} txs', len(block_htable_txs_cached), block_num, len(expected_block_txs_cache[block_num]))
                            expected_txs_cached_status[block_num] = all(tx_hash in block_htable_txs_cached for tx_hash in expected_block_txs_cache[block_num])
                    if all(expected_txs_cached_status.values()):
                        self.logger.info("✅ All txs cached in block range {} to {}", event.args.begin, event.args.end)
                        block_txs_cached = True
                if blocks_cached and block_txs_cached:
                    break
                await asyncio.sleep(polling_interval)
                count += 1
                if count > 30:
                    self.logger.warning("Timeout waiting for block and tx receipt cache in range {} to {}", event.args.begin, event.args.end)
                    return
            self.logger.info("✅ Block and tx receipt cache found in Redis in range {} to {}", event.args.begin, event.args.end)
            # check address is data market address
            if event.args.dataMarketAddress == self.data_market_address:
                worker_epoch_released_event = EpochReleasedEvent(
                    epochId=event.args.epochId,
                    begin=event.args.begin,
                    end=event.args.end,
                    timestamp=event.args.timestamp
                )
                dramatiq.broker.get_broker().enqueue(
                    dramatiq.Message(
                        queue_name=self._event_detection_q,
                        actor_name='handleEvent',
                        args=('EpochReleased', worker_epoch_released_event.json()),
                        kwargs={},
                        options={},
                    ),
                )
                self.logger.info("✅ Sent message to handleEvent for epoch {} in range {} to {}", event.args.epochId, event.args.begin, event.args.end)
            else:
                self.logger.warning("Epoch released event data market address {} does not match expected data market address {}", event.args.dataMarketAddress, self.data_market_address)
            # Remove task from tracking
            if task_id in self._cache_check_tasks:
                del self._cache_check_tasks[task_id]
        except Exception as e:
            self.logger.error("Error in cache checking task: {}", str(e))
            if task_id in self._cache_check_tasks:
                del self._cache_check_tasks[task_id]

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
            if event.event == 'DayStartedEvent':
                self.logger.info(
                    "Day Started - DataMarket: {}, DayId: {}, Timestamp: {}",
                    event.args.dataMarketAddress,
                    event.args.dayId,
                    event.args.timestamp
                )
                # set current day in redis
                await self._redis.set(
                    "current_day",
                    str(event.args.dayId),
                )
            else:
                self.logger.info(
                    "Epoch Released - DataMarket: {}, EpochId: {}, Begin: {}, End: {}, Timestamp: {}",
                    event.args.dataMarketAddress,
                    event.args.epochId,
                    event.args.begin,
                    event.args.end,
                    event.args.timestamp
                )
                
                # # Start background task to check cache and send message when ready
                task_id = f"{event.args.epochId}_{event.args.begin}_{event.args.end}"
                if task_id not in self._cache_check_tasks:
                    self._cache_check_tasks[task_id] = asyncio.create_task(
                        self._check_cache_and_send(event, polling_interval=1)
                    )
            
        return events

    async def detect_events(self):
        first_run = True
        """Main event detection loop"""
        current_day = await self.contract.functions.dayCounter(self.data_market_address).call()
        # set current day in redis
        await self._redis.set(
            "current_day",
            str(current_day),
        )
        try:
            while True:
                try:
                    current_block = await self._powerloom_rpc_helper.get_current_block_number()
                    
                    if not self._last_processed_block:
                        last_processed_block_data = await self._redis.get(
                            event_detector_last_processed_block(self.settings.namespace),
                        )

                        if last_processed_block_data:
                            self._last_processed_block = int(last_processed_block_data)
                            self.logger.info("Loaded last processed block from redis: {}", self._last_processed_block)
                            first_run = False
                        else:
                            self._last_processed_block = current_block - 1
                            self.logger.info("Starting to listen from block {}", self._last_processed_block)
                    
                    if current_block > self._last_processed_block:
                        if current_block - self._last_processed_block >= 10:
                            self.logger.warning("Last processed block is too far behind current block, processing from {} to {}", self._last_processed_block + 1, current_block)
                            self._last_processed_block = current_block - 10
                        # Get events from last processed to current
                        await self.get_events(
                            from_block=self._last_processed_block + 1 if not first_run else self._last_processed_block,
                            to_block=current_block
                        )
                        self._last_processed_block = current_block
                        await self._redis.set(
                            event_detector_last_processed_block(self.settings.namespace),
                            str(self._last_processed_block),
                        )
                    
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