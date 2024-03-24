from operator import attrgetter
from typing import List
from app.collections.dna import DNASequenceCollection
from app.models.dna import DNASequence
from app.services.db import DBService


def dna_sequences_batch_update(batch_id: int, dna_list: List[DNASequence]):
    with DBService() as db, DNASequenceCollection() as dna:
        dna_batch = dna.update(dna_list)
        db.update_batch(batch_id, list(map(attrgetter("id"), dna_batch)))
