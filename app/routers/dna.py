from typing import List, Literal, Optional
from fastapi import APIRouter, BackgroundTasks

from app.collections.dna import DNASequenceCollection
from app.models.dna import DNABatchResponse, DNABatchStatus, DNASequence
from app.routers.tags import Tags
from app.services.db import DBService
from app.tasks import dna_sequences_batch_update

router = APIRouter()


@router.get(
    "/dna",
    operation_id="listDnaSequences",
    summary="Get all DNA Sequences",
    tags=[Tags.DNA],
)
def list_dna_sequences() -> List[DNASequence]:
    with DNASequenceCollection() as dna:
        return list(dna)


@router.get(
    "/dna/{id}",
    operation_id="getDnaSequence",
    summary="Get a DNA Sequence by ID",
    tags=[Tags.DNA],
)
def get_dna_sequence(id: int) -> DNASequence:
    with DNASequenceCollection() as dna:
        return dna[id]


@router.get(
    "/dna/search/",
    operation_id="dnaSequenceSearch",
    summary="Search for DNA Sequences by pattern",
    tags=[Tags.DNA],
)
def dna_sequence_search(pattern: str) -> List[DNASequence]:
    with DNASequenceCollection() as dna:
        return list(dna.search(pattern))


@router.get(
    "/dna/batch/{id}",
    operation_id="listBatch",
    summary="Get DNA Sequences by Batch ID",
    tags=[Tags.DNA],
)
def list_batch(id: int) -> List[DNASequence]:
    with DNASequenceCollection() as dna:
        return list(dna.by_batch(id))


@router.get(
    "/dna/batch/{id}/status",
    operation_id="getBatchStatus",
    summary="Get upload status of DNA Sequence Batch",
    tags=[Tags.DNA],
)
def get_batch_status(id: int) -> Optional[DNABatchStatus]:
    with DBService() as db:
        response = db.get_batch_status(id)

        if response:
            return DNABatchStatus(status=response)


@router.post(
    "/dna",
    operation_id="createDNASequence",
    summary="Create a DNA Sequence",
    tags=[Tags.DNA],
)
def create_dna_sequence(dna: DNASequence):
    with DNASequenceCollection() as sequences:
        return sequences.add(dna)


@router.post(
    "/dna:bulk",
    operation_id="bulkCreateDNASequences",
    summary="Create DNA Sequences in bulk",
    tags=[Tags.DNA],
)
def bulk_create_dna_sequences(dna: List[DNASequence]):
    with DNASequenceCollection() as sequences:
        return sequences.update(dna)


@router.post(
    "/dna/batch",
    operation_id="createDNASequenceBatch",
    summary="Upload a DNA Sequence Batch for processing",
    tags=[Tags.DNA],
)
def create_dna_sequence_batch(
    dna: List[DNASequence], background_tasks: BackgroundTasks
) -> DNABatchResponse:
    with DBService() as db:
        batch_id = db.init_batch()

        # queues a task for uploading the batch from background processing
        background_tasks.add_task(
            dna_sequences_batch_update, batch_id=batch_id, dna_list=dna
        )

        return DNABatchResponse(id=batch_id)
