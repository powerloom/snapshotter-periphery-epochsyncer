from pydantic import BaseModel

class EpochReleasedEvent(BaseModel):
    begin: int
    end: int
    epochId: int
    timestamp: int


class SnapshotBatchSubmittedEvent(BaseModel):
    """
    Event model for when a snapshot batch is finalized.
    """
    epochId: int
    batchCid: str
    timestamp: int
    transactionHash: str
