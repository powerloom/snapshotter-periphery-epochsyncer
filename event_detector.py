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
from utils.models.message_models import EpochReleasedEvent, SnapshotBatchSubmittedEvent


class EpochEventDetector:
    _redis: aioredis.Redis
    _broker: RedisBroker
    _cache_check_tasks: dict[str, asyncio.Task]
    _last_processed_block: int
    _last_processed_block_source_chain: int

    def __init__(self):
        self.settings = get_core_config()
        self._powerloom_rpc_helper = RpcHelper(self.settings.powerloom_rpc)
        self._source_rpc_helper = RpcHelper(self.settings.source_rpc)
        self.logger = logger.bind(module='EpochEventDetector')
        self._cache_check_tasks = {}
        with open('utils/abi/ProtocolContract.json', 'r') as f:
            self.protocol_state_abi = json.load(f)
        self._event_detection_q = f'powerloom-event-detector_{self.settings.namespace}_{self.settings.instance_id}'
        
        # Performance optimization settings
        self.BATCH_SIZE = 50  # Process blocks in batches
        self.MAX_CONCURRENT_CACHE_CHECKS = 20  # Limit concurrent cache checks
        self.ADAPTIVE_POLLING = True  # Use adaptive polling intervals
        self.MIN_POLLING_INTERVAL = 0.1  # Minimum polling interval (100ms)
        self.MAX_POLLING_INTERVAL = 5.0  # Maximum polling interval (5s)
        self.current_polling_interval = 1.0  # Start with 1s
        
    async def init(self):
        self._last_processed_block = 0
        self._last_processed_block_source_chain = 0
        """Initialize RPC connection"""
        await self._powerloom_rpc_helper.init()
        await self._source_rpc_helper.init()
        # Set up the contract and event ABIs
        self.contract = self._powerloom_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.to_checksum_address(self.settings.protocol_state_contract_address),
            abi=self.protocol_state_abi,
        )
        self.data_market_address = Web3.to_checksum_address(self.settings.data_market_contract_address)

        EVENTS_ABI = {
            'DayStartedEvent': self.contract.events.DayStartedEvent._get_event_abi(),
            'SnapshotBatchSubmitted': self.contract.events.SnapshotBatchSubmitted._get_event_abi(),
        }

        EVENT_SIGS = {
            'DayStartedEvent': 'DayStartedEvent(address,uint256,uint256)',
            'SnapshotBatchSubmitted': 'SnapshotBatchSubmitted(address,string,uint256,uint256)',
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

    async def _batch_get_blocks_data(self, block_numbers: list[int]) -> dict[int, dict]:
        """Fetch multiple blocks data in parallel"""
        tasks = []
        for block_num in block_numbers:
            task = self._source_rpc_helper.eth_get_block(block_num)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        blocks_data = {}
        for i, (block_num, result) in enumerate(zip(block_numbers, results)):
            if isinstance(result, Exception):
                self.logger.warning("Failed to fetch block {}: {}", block_num, str(result))
                # Use block number as timestamp fallback
                blocks_data[block_num] = {'timestamp': hex(block_num), 'number': hex(block_num)}
            else:
                blocks_data[block_num] = result
        
        return blocks_data

    async def _batch_check_complete_cache_status(self, block_numbers: list[int]) -> dict[int, dict]:
        """Check both block cache AND transaction cache completeness for multiple blocks"""
        # First get block details and transaction lists
        pipeline = self._redis.pipeline()
        cached_blocks_zset = block_cache_key(self.settings.namespace)
        
        for block_num in block_numbers:
            pipeline.zrangebyscore(
                name=cached_blocks_zset,
                min=block_num,
                max=block_num,
                withscores=True,
                score_cast_func=int
            )
        
        block_results = await pipeline.execute()
        
        # Extract transaction lists from cached blocks
        block_tx_mapping = {}
        for i, block_num in enumerate(block_numbers):
            block_details = block_results[i]
            if block_details:
                for block_data, _ in block_details:
                    try:
                        parsed_block = json.loads(block_data)
                        block_tx_mapping[block_num] = parsed_block.get('transactions', [])
                    except (json.JSONDecodeError, KeyError):
                        block_tx_mapping[block_num] = []
        
        # Now check transaction cache completeness
        pipeline = self._redis.pipeline()
        for block_num in block_tx_mapping:
            pipeline.hkeys(block_tx_htable_key(self.settings.namespace, block_num))
        
        tx_cache_results = await pipeline.execute()
        
        # Verify completeness
        complete_status = {}
        for i, block_num in enumerate(block_tx_mapping.keys()):
            expected_txs = set(block_tx_mapping[block_num])
            cached_txs = set(tx_cache_results[i]) if i < len(tx_cache_results) else set()
            
            complete_status[block_num] = {
                'block_cached': True,
                'tx_cached': expected_txs.issubset(cached_txs),
                'complete': expected_txs.issubset(cached_txs),
                'expected_tx_count': len(expected_txs),
                'cached_tx_count': len(cached_txs),
                'missing_txs': expected_txs - cached_txs
            }
        
        # Add blocks that aren't cached at all
        for block_num in block_numbers:
            if block_num not in complete_status:
                complete_status[block_num] = {
                    'block_cached': False,
                    'tx_cached': False,
                    'complete': False,
                    'expected_tx_count': 0,
                    'cached_tx_count': 0,
                    'missing_txs': set()
                }
        
        return complete_status

    async def _optimized_cache_and_send(
        self, 
        blocks_data: dict[int, dict], 
        max_wait_time: int = 30
    ) -> None:
        """Optimized batch cache checking with COMPLETE verification (blocks + transactions)"""
        block_numbers = list(blocks_data.keys())
        
        # Quick initial COMPLETE cache check
        cache_status = await self._batch_check_complete_cache_status(block_numbers)
        ready_blocks = [
            block_num for block_num, status in cache_status.items() 
            if status['complete']
        ]
        
        if ready_blocks:
            # Send messages for completely ready blocks immediately
            await self._batch_send_epoch_messages(ready_blocks, blocks_data)
            self.logger.info(
                "✅ Immediately processed {} blocks with complete cache", 
                len(ready_blocks)
            )
        
        # For remaining blocks, use adaptive waiting with complete verification
        pending_blocks = [
            block_num for block_num in block_numbers 
            if not cache_status[block_num]['complete']
        ]
        
        if pending_blocks:
            self.logger.info(
                "⏳ Waiting for complete cache (blocks + transactions) for {} blocks", 
                len(pending_blocks)
            )
            await self._adaptive_wait_for_complete_cache(pending_blocks, blocks_data, max_wait_time)

    async def _batch_send_epoch_messages(self, block_numbers: list[int], blocks_data: dict[int, dict]):
        """Send epoch messages for multiple blocks in batch"""
        messages = []
        
        for block_num in block_numbers:
            block_data = blocks_data[block_num]
            timestamp = int(block_data.get('timestamp', hex(block_num)), 16)
            
            worker_epoch_released_event = EpochReleasedEvent(
                epochId=block_num,
                begin=block_num,
                end=block_num,
                timestamp=timestamp
            )
            
            message = dramatiq.Message(
                queue_name=self._event_detection_q,
                actor_name='handleEvent',
                args=('EpochReleased', worker_epoch_released_event.json()),
                kwargs={},
                options={},
            )
            messages.append(message)
        
        # Send all messages in batch
        broker = dramatiq.broker.get_broker()
        for message in messages:
            broker.enqueue(message)
        
        self.logger.info(
            "✅ Sent {} epoch messages for blocks: {}", 
            len(messages), 
            sorted(block_numbers)
        )

    async def _adaptive_wait_for_complete_cache(
        self, 
        block_numbers: list[int], 
        blocks_data: dict[int, dict], 
        max_wait_time: int
    ):
        """Adaptive waiting strategy for COMPLETE cache availability (blocks + transactions)"""
        start_time = asyncio.get_event_loop().time()
        check_interval = 0.1  # Start with 100ms
        max_check_interval = 2.0
        
        while block_numbers and (asyncio.get_event_loop().time() - start_time) < max_wait_time:
            await asyncio.sleep(check_interval)
            
            # Check COMPLETE cache status for remaining blocks
            cache_status = await self._batch_check_complete_cache_status(block_numbers)
            ready_blocks = [
                block_num for block_num, status in cache_status.items() 
                if status['complete']
            ]
            
            if ready_blocks:
                # Send messages for newly ready blocks
                await self._batch_send_epoch_messages(ready_blocks, blocks_data)
                # Remove from pending list
                block_numbers = [block_num for block_num in block_numbers if block_num not in ready_blocks]
                
                self.logger.info(
                    "✅ Processed {} more blocks with complete cache, {} remaining", 
                    len(ready_blocks), 
                    len(block_numbers)
                )
            else:
                # Log status for debugging
                incomplete_blocks = [
                    block_num for block_num, status in cache_status.items() 
                    if not status['complete']
                ]
                if incomplete_blocks:
                    sample_status = cache_status[incomplete_blocks[0]]
                    self.logger.debug(
                        "Waiting for {} blocks - Sample block {}: block_cached={}, "
                        "tx_cached={}, expected_txs={}, cached_txs={}", 
                        len(incomplete_blocks),
                        incomplete_blocks[0],
                        sample_status['block_cached'],
                        sample_status['tx_cached'], 
                        sample_status['expected_tx_count'],
                        sample_status['cached_tx_count']
                    )
            
            # Exponential backoff for check interval
            check_interval = min(check_interval * 1.2, max_check_interval)
        
        if block_numbers:
            # Final status check for timeout blocks
            final_status = await self._batch_check_complete_cache_status(block_numbers)
            for block_num in block_numbers:
                status = final_status[block_num]
                self.logger.warning(
                    "Timeout waiting for complete cache for block {}: "
                    "block_cached={}, tx_cached={}, expected_txs={}, cached_txs={}, missing_txs={}", 
                    block_num,
                    status['block_cached'],
                    status['tx_cached'],
                    status['expected_tx_count'], 
                    status['cached_tx_count'],
                    len(status['missing_txs'])
                )

    async def _adaptive_polling_adjustment(self, blocks_processed: int, processing_time: float):
        """Adjust polling interval based on performance metrics"""
        if not self.ADAPTIVE_POLLING:
            return
        
        # Calculate blocks per second
        blocks_per_second = blocks_processed / max(processing_time, 0.001)
        
        # Adjust polling interval based on throughput
        if blocks_per_second > 10:  # High throughput
            self.current_polling_interval = max(
                self.current_polling_interval * 0.8, 
                self.MIN_POLLING_INTERVAL
            )
        elif blocks_per_second < 1:  # Low throughput
            self.current_polling_interval = min(
                self.current_polling_interval * 1.5, 
                self.MAX_POLLING_INTERVAL
            )
        
        self.logger.debug(
            "Adaptive polling: {:.2f} blocks/s, interval: {:.2f}s", 
            blocks_per_second, 
            self.current_polling_interval
        )

    async def _handle_snapshot_batch_submitted(self, event):
        self.logger.info(f"Handling snapshot batch submitted event: {event}")
        self.logger.info(f"Comparing data market addresses - Event: {event.args.dataMarketAddress}, Expected: {self.data_market_address}")
        try:
            worker_snapshot_batch_submitted_event = SnapshotBatchSubmittedEvent(
                epochId=event.args.epochId,
                batchCid=event.args.batchCid,
                timestamp=event.args.timestamp,
                transactionHash=event.transactionHash.hex()
            )
            self.logger.info(f"Created event object: {worker_snapshot_batch_submitted_event}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=self._event_detection_q,
                    actor_name='handleEvent',
                    args=('SnapshotBatchSubmitted', worker_snapshot_batch_submitted_event.model_dump_json()),
                    kwargs={},
                    options={}
                ),
            )
            self.logger.info(
                "✅ Sent message to handleEvent for snapshot batch {} in epoch {}", 
                event.args.batchCid, 
                event.args.epochId
            )
        except Exception as e:
            self.logger.error("Error in snapshot batch submitted event: {}", str(e))
            self.logger.exception("Full traceback:")

    async def detect_blocks(self):
        """Optimized source chain block detection with complete cache verification"""
        self.logger.info("Starting optimized source chain block detection with complete cache verification")
        first_run = True
        
        try:
            while True:
                try:
                    processing_start_time = asyncio.get_event_loop().time()
                    current_block = await self._source_rpc_helper.get_current_block_number()
                    
                    if not self._last_processed_block_source_chain:
                        # Load from Redis or start from recent block
                        last_processed_block_data = await self._redis.get(
                            f"{event_detector_last_processed_block(self.settings.namespace)}:SourceChain",
                        )

                        if last_processed_block_data:
                            self._last_processed_block_source_chain = int(last_processed_block_data)
                            self.logger.info(
                                "Loaded last processed source chain block from redis: {}", 
                                self._last_processed_block_source_chain
                            )
                            first_run = False
                        else:
                            self._last_processed_block_source_chain = current_block - 1
                            self.logger.info(
                                "Starting to listen from source chain block {}", 
                                self._last_processed_block_source_chain
                            )
                    
                    if current_block > self._last_processed_block_source_chain:
                        # Calculate blocks to process
                        start_block = (self._last_processed_block_source_chain + 1 
                                       if not first_run 
                                       else self._last_processed_block_source_chain)
                        end_block = current_block
                        
                        # Limit catch-up to prevent overwhelming
                        if end_block - start_block > 100:
                            self.logger.warning(
                                "Large block gap detected ({} blocks), limiting to 100 blocks", 
                                end_block - start_block
                            )
                            start_block = end_block - 100

                        blocks_to_process = list(range(start_block, end_block + 1))
                        
                        # Process in batches for better performance
                        for i in range(0, len(blocks_to_process), self.BATCH_SIZE):
                            batch = blocks_to_process[i:i + self.BATCH_SIZE]
                            
                            # Fetch block data in parallel
                            blocks_data = await self._batch_get_blocks_data(batch)
                            
                            # Use optimized COMPLETE cache checking and sending
                            await self._optimized_cache_and_send(blocks_data)
                            
                            self.logger.info(
                                "Processed batch of {} blocks: {} to {}", 
                                len(batch), 
                                batch[0], 
                                batch[-1]
                            )

                        self._last_processed_block_source_chain = current_block
                        await self._redis.set(
                            f"{event_detector_last_processed_block(self.settings.namespace)}:SourceChain",
                            str(self._last_processed_block_source_chain),
                        )
                        
                        # Adaptive polling adjustment
                        processing_time = asyncio.get_event_loop().time() - processing_start_time
                        await self._adaptive_polling_adjustment(len(blocks_to_process), processing_time)
                    
                    # Wait before next check
                    first_run = False
                    await asyncio.sleep(self.current_polling_interval)

                except Exception as e:
                    self.logger.opt(exception=True).error("Error processing source chain blocks: {}", str(e))
                    
                    await asyncio.sleep(5)  # Wait before retrying
                    raise e

        except Exception as e:
            self.logger.error("Fatal error in source chain block listener: {}", str(e))
            raise

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
            if event.args.dataMarketAddress != self.data_market_address:
                self.logger.warning(
                    "Data market address {} does not match expected data market address {}", 
                    event.args.dataMarketAddress, 
                    self.data_market_address
                )
                continue

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
            elif event.event == 'SnapshotBatchSubmitted':
                self.logger.info(
                    "Snapshot Batch Submitted - DataMarket: {}, EpochId: {}, "
                    "BatchCid: {}, Timestamp: {}, TransactionHash: {}",
                    event.args.dataMarketAddress,
                    event.args.epochId,
                    event.args.batchCid,
                    event.args.timestamp,
                    event.transactionHash.hex()
                )
                await self._handle_snapshot_batch_submitted(event)
            
        return events

    async def detect_events(self):
        self.logger.info("Starting event detection test test")
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
                            self.logger.warning(
                                "Last processed block is too far behind current block, "
                                "processing from {} to {}", 
                                self._last_processed_block + 1, 
                                current_block
                            )
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
    # Run both tasks concurrently
    tasks = [
        asyncio.create_task(detector.detect_blocks()),
        asyncio.create_task(detector.detect_events())
    ]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error("Fatal error in main: {}", str(e))
        raise

if __name__ == "__main__":
    asyncio.run(main())