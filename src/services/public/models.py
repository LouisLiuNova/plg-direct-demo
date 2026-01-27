from pydantic import BaseModel, Field


class Payload(BaseModel):
    ts: float = Field(..., description="Timestamp of the payload")
    file: str = Field(..., description="Path to the log file")
