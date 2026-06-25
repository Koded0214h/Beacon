from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class AgentStatus(BaseModel):
    agent_id: str = Field(min_length=1)
    status: str
    timestamp: datetime
    metadata: Optional[dict] = None

    