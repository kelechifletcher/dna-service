from dataclasses import dataclass
from typing import Optional

from app.services.db import DBService


@dataclass
class Context:
    db: Optional[DBService] = None
