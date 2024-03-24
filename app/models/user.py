from typing import Optional

from humps import camelize
from pydantic import BaseModel


class User(BaseModel):
    id: Optional[int]
    benchling_id: str
    name: str
    handle: str

    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True
        orm_mode = True
