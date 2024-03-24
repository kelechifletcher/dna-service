from contextlib import AbstractContextManager
from itertools import repeat
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
    text,
    update,
)
from app.config import Config
from app.models.dna import Status

from app.utils import singleton


@singleton
class DBService(AbstractContextManager):
    """
    Singleton class encapsulating a database connection.
    """
    _metadata: MetaData = MetaData()

    # data definition language (DDL)
    # DNA sequence definition
    dna_sequence: Table = Table(
        "dna_sequence",
        _metadata,
        Column("id", Integer, Identity(), primary_key=True),
        Column("benchling_id", String(16), unique=True),
        Column("creator_id", Integer, ForeignKey("user.id")),
        Column("name", String(70)),
        Column("created_at", DateTime),
        Column("bases", String(collation="C")),
    )

    # User definition
    user: Table = Table(
        "user",
        _metadata,
        Column("id", Integer, Identity(), primary_key=True),
        Column("benchling_id", String(16), unique=True),
        Column("name", String(70)),
        Column("handle", String(16)),
    )

    # Batch definition
    batch: Table = Table(
        "batch",
        _metadata,
        Column("id", Integer, Identity(), primary_key=True),
        Column("status", Enum(Status), default=Status.INITIATED),
    )

    # DNA <-> Batch mapping definition (many-to-many)
    dna_batch: Table = Table(
        "dna_batch",
        _metadata,
        Column("batch_id", Integer, ForeignKey("batch.id")),
        Column("dna_sequence_id", Integer, ForeignKey("dna_sequence.id")),
    )

    # Trigram index on DNA sequences bases; optimized infix pattern matching
    dna_sequence_bases_trgm: Index = Index(
        "dna_sequence_bases_trgm",
        dna_sequence.c.bases,
        postgresql_using="gin",
        postgresql_ops={
            "bases": "gin_trgm_ops",
        },
    )

    def __init__(self, config=Config()) -> None:
        self._engine = create_engine(
            f"postgresql+psycopg://{config.db_username}:{config.db_password}@{config.db_host}:{config.db_port}/dna",
            pool_size=5,
        )

    def __enter__(self) -> "DBService":
        return self

    def __exit__(self, *_) -> None:
        return self.exit()

    def execute(self, statement, *args, **kwargs):
        with self._engine.connect() as connection, connection.begin():
            return connection.execute(statement, *args, **kwargs)

    def create_all(self):
        return self._metadata.create_all(self._engine)

    def drop_all(self):
        return self._metadata.drop_all(self._engine)

    def init_batch(self) -> int:
        batch = self.batch
        return self.execute(insert(batch).returning(batch.c.id)).scalar()

    def get_batch_status(self, id: int) -> Optional[Status]:
        batch = self.batch
        return self.execute(
            select(batch.c.status).where(batch.c.id == id)
        ).scalar_one_or_none()

    def update_batch(self, id: int, dna_ids: list[int]):
        batch = self.batch
        dna_batch = self.dna_batch

        try:
            # map batch to sequences
            self.execute(insert(dna_batch).values(list(zip(repeat(id), dna_ids))))

        except Exception as e:
            self.execute(
                update(batch).where(batch.c.id == id).values(status=Status.FAILED)
            )

        # update batch status
        self.execute(
            update(batch).where(batch.c.id == id).values(status=Status.COMPLETED)
        )

    def add_pg_trgm(self):
        self.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

    def exit(self):
        return self._engine.dispose()
