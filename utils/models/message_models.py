from pydantic import BaseModel

class EpochReleasedEvent(BaseModel):
    begin: int
    end: int
    epochId: int
    timestamp: int
