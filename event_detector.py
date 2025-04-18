import asyncio
from web3 import AsyncWeb3, AsyncHTTPProvider
from utils.logging import logger, configure_file_logging
from config.loader import get_core_config
from utils.rpc import RpcHelper, get_event_sig_and_abi
from web3 import Web3
import json
class EpochEventDetector:
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

        # Get event signature and ABI
        self.event_signatures = {
            'EpochReleased': 'EpochReleased(address,uint256,uint256,uint256,uint256)'
        }
        
        # Get the event ABI from the contract
        epoch_released_abi = self.contract.events.EpochReleased._get_event_abi()
        
        # Create the event ABIs dict keyed by the same name as in event_signatures
        event_abis = {
            'EpochReleased': epoch_released_abi
        }
        
        # Get the event signature hash and ABI dict keyed by hash
        event_sig_list, event_abi_by_hash = get_event_sig_and_abi(self.event_signatures, event_abis)
        
        # Store the first (and only) signature hash since we only have one event
        self.event_sig_hash = event_sig_list[0]
        self.event_abi_dict = event_abi_by_hash

    async def get_events(self, from_block: int, to_block: int):
        """Get EpochReleased events in block range"""
        self.logger.info("Getting events from block {} to block {}", from_block, to_block)
        events = await self._powerloom_rpc_helper.get_events_logs(
            contract_address=self.settings.protocol_state_contract_address,
            to_block=to_block,
            from_block=from_block,
            topics=[self.event_sig_hash],
            event_abi=self.event_abi_dict
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
        
        return events

    async def detect_events(self):
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
                            from_block=self._last_processed_block + 1,
                            to_block=current_block
                        )
                        self._last_processed_block = current_block
                    
                    # Wait before next check
                    await asyncio.sleep(self.settings.powerloom_rpc.polling_interval)

                except Exception as e:
                    self.logger.error("Error processing events: {}", str(e))
                    await asyncio.sleep(5)  # Wait before retrying

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
