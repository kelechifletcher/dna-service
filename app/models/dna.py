from datetime import datetime
from enum import Enum
from typing import Optional

from humps import camelize
from pydantic import BaseModel, validator

from .user import User

# character set of DNA symbols as defined by IUPAC
IUPAC_NUCLEOTIDE_SYMBOLS = set("ACGTUWSMKRYBDHVN".lower())


class DNASequence(BaseModel):
    id: Optional[int]
    benchling_id: str
    name: str
    created_at: datetime
    bases: str
    creator: Optional[User]

    @validator("bases")
    def check_nucleotide_symbols(cls, v):
        """
        Check that `bases` is entirely composed of IUPAC
        nucleotide symbols; else raise `ValueError`
        """
        if all(map(is_iupac, v.lower())):
            return v

        else:
            raise ValueError("invalid nucleotide symbol")

    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True
        orm_mode = True


class Status(str, Enum):
    COMPLETED = "completed"
    INITIATED = "initiated"
    FAILED = "failed"


class DNABatchResponse(BaseModel):
    id: int


class DNABatchStatus(BaseModel):
    status: Status


def is_iupac(sym: str) -> bool:
    return sym in IUPAC_NUCLEOTIDE_SYMBOLS
