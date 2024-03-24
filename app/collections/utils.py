from functools import partial
from typing import Dict, List

from pydantic import BaseModel


def as_records(objs: List[BaseModel], exclude=None) -> List[Dict]:
    return list(map(partial(BaseModel.dict, exclude=exclude), objs))
