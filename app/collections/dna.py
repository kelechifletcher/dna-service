from operator import attrgetter
from typing import Collection, Dict, Iterator, List, Optional

from sqlalchemy import DateTime, Table, column, func, select, values
from sqlalchemy.dialects.postgresql import insert

from app.collections.user import UserCollection
from app.collections.utils import as_records
from app.models.dna import DNASequence
from app.services.db import DBService

dna_sequence: Table = DBService.dna_sequence
dna_batch: Table = DBService.dna_batch
user: Table = DBService.user


class DNASequenceCollection(Collection[DNASequence]):
    """
    `DNASequence` collection that acts as the DNA controller of the API.
    """
    def __init__(self, db: DBService = DBService()) -> None:
        self._db = db
        self._users = UserCollection(db)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        ...

    def __contains__(self, dna: DNASequence) -> bool:
        return bool(
            self._db.execute(
                select(func.count(dna_sequence.c.id)).where(dna_sequence.c.id == dna.id)
            ).scalar()
        )

    def __getitem__(self, id: int) -> DNASequence:
        return DNASequence.from_orm(
            self._db.execute(
                select(
                    dna_sequence, func.row_to_json(user.table_valued()).label("creator")
                )
                .join_from(dna_sequence, user)
                .where(dna_sequence.c.id == id)
            ).one()
        )

    def __iter__(self) -> Iterator[DNASequence]:
        return iter(
            map(
                DNASequence.from_orm,
                self._db.execute(
                    select(
                        dna_sequence,
                        func.row_to_json(user.table_valued()).label("creator"),
                    ).join_from(dna_sequence, user)
                ),
            )
        )

    def __len__(self) -> int:
        return self._db.execute(select(func.count(dna_sequence.c.id))).scalar()

    def add(self, dna: DNASequence) -> Optional[DNASequence]:
        # insertion statement (as CTE)
        cte = (
            insert(dna_sequence)
            .on_conflict_do_nothing(index_elements=[dna_sequence.c.benchling_id])
            .values(
                benchling_id=dna.benchling_id,
                creator_id=self._users.get(
                    dna.creator.id or dna.creator.benchling_id, dna.creator
                ).id,
                name=dna.name,
                created_at=dna.created_at,
                bases=dna.bases,
            )
            .returning(dna_sequence)
            .cte()
        )

        # join newly inserted sequences with user
        statement = select(
            cte, func.row_to_json(user.table_valued()).label("creator")
        ).join_from(cte, user)

        record = self._db.execute(statement).one_or_none()

        if record:
            return DNASequence.from_orm(record)

    def update(self, dna: List[DNASequence]):
        # add users
        self._users.update(map(attrgetter("creator"), dna))

        # format dna sequence records
        new_dna = values(
            column("benchling_id"),
            column("creator_benchling_id"),
            column("name"),
            column("created_at", DateTime),
            column("bases"),
            name="new_dna",
        ).data(list(map(_as_tuple, as_records(dna, exclude={"id"}))))

        # construct bulk insert statement (as CTE)
        cte = (
            insert(dna_sequence)
            .on_conflict_do_nothing(index_elements=[dna_sequence.c.benchling_id])
            .from_select(
                [
                    dna_sequence.c.benchling_id,
                    dna_sequence.c.creator_id,
                    dna_sequence.c.name,
                    dna_sequence.c.created_at,
                    dna_sequence.c.bases,
                ],
                select(
                    new_dna.c.benchling_id,
                    user.c.id,
                    new_dna.c.name,
                    new_dna.c.created_at,
                    new_dna.c.bases,
                ).join_from(
                    new_dna,
                    user,
                    new_dna.c.creator_benchling_id == user.c.benchling_id,
                ),
            )
            .returning(dna_sequence)
            .cte()
        )

        # join newly inserted sequences with user
        statement = select(
            cte, func.row_to_json(user.table_valued()).label("creator")
        ).join_from(cte, user)

        return list(map(DNASequence.from_orm, self._db.execute(statement)))

    def search(self, pattern: str) -> Iterator[DNASequence]:
        cursor = self._db.execute(
            select(dna_sequence, func.row_to_json(user.table_valued()).label("creator"))
            .join_from(dna_sequence, user)
            .where(dna_sequence.c.bases.ilike(f"%{pattern}%"))
        )

        for record in cursor:
            yield DNASequence.from_orm(record)

    def by_batch(self, batch_id: int) -> Iterator[DNASequence]:
        cursor = self._db.execute(
            select(dna_sequence, func.row_to_json(user.table_valued()).label("creator"))
            .join_from(dna_sequence, user)
            .join_from(dna_sequence, dna_batch)
            .where(dna_batch.c.batch_id == batch_id)
        )

        for record in cursor:
            yield DNASequence.from_orm(record)

    def by_user(self, user_id: int) -> Iterator[DNASequence]:
        cursor = self._db.execute(
            select(dna_sequence, func.row_to_json(user.table_valued()).label("creator"))
            .join_from(dna_sequence, user)
            .where(user.c.id == user_id)
        )

        for record in cursor:
            yield DNASequence.from_orm(record)


def _as_tuple(r: Dict) -> tuple:
    return (
        r["benchling_id"],
        r["creator"]["benchling_id"],
        r["name"],
        r["created_at"],
        r["bases"],
    )
