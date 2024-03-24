from functools import singledispatchmethod
from typing import Collection, Iterator, List, Optional
from pydantic import BaseModel

from sqlalchemy import Table, func, select
from sqlalchemy.dialects.postgresql import insert
from app.collections.utils import as_records

from app.models.user import User
from app.services.db import DBService

user: Table = DBService.user


class UserCollection(Collection[User]):
    """
    `User` collection that acts as the User controller of the API.
    """

    def __init__(self, db: DBService = DBService()) -> None:
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, *_):
        ...

    def __contains__(self, obj: User) -> bool:
        return bool(
            self._db.execute(
                select(func.count(user.c.id)).where(user.c.id == obj.id)
            ).scalar()
        )

    def __getitem__(self, id: str) -> User:
        return User.from_orm(
            self._db.execute(select(user).where(user.c.id == id)).one()
        )

    def __iter__(self) -> Iterator[User]:
        return iter(map(User.from_orm, self._db.execute(select(user))))

    def __len__(self) -> int:
        return self._db.execute(select(func.count(user.c.id))).scalar()

    def add(self, obj: User) -> User:
        return User.from_orm(
            self._db.execute(
                insert(user)
                .on_conflict_do_nothing(index_elements=[user.c.benchling_id])
                .values(benchling_id=obj.benchling_id, name=obj.name, handle=obj.handle)
                .returning(user)
            ).one()
        )

    def update(self, users: List[User]) -> User:
        return list(
            map(
                User.from_orm,
                self._db.execute(
                    insert(user)
                    .on_conflict_do_nothing(index_elements=[user.c.benchling_id])
                    .values(as_records(users, exclude={"id"}))
                    .returning(user)
                ),
            )
        )

    @singledispatchmethod
    def get(self, id: int, default: Optional[User] = None) -> User:
        record = self._db.execute(select(user).where(user.c.id == id)).one_or_none()

        if record:
            return User.from_orm(record)

        return self.add(default)

    @get.register
    def _(self, id: str, default: Optional[User] = None) -> User:
        record = self._db.execute(select(user).where(user.c.benchling_id == id)).one_or_none()

        if record:
            return User.from_orm(record)

        return self.add(default)
