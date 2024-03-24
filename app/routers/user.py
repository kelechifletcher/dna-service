from typing import List
from fastapi import APIRouter
from app.collections.dna import DNASequenceCollection
from app.collections.user import UserCollection
from app.models.dna import DNASequence

from app.models.user import User
from app.routers.tags import Tags

router = APIRouter()


@router.get(
    "/users", operation_id="listUsers", summary="Get all Users", tags=[Tags.USER]
)
def list_all_users() -> List[User]:
    with UserCollection() as users:
        return list(users)


@router.get(
    "/users/{id}",
    operation_id="Get User by ID",
    summary="Get a User by ID",
    tags=[Tags.USER],
)
def get_user(id: int) -> User:
    with UserCollection() as users:
        return users[id]


@router.get(
    "/users/{id}/dna",
    operation_id="listSequencesByUser",
    summary="Get all DNA Sequences by User ID",
    tags=[Tags.USER],
)
def list_sequences_by_user(id: int) -> DNASequence:
    with DNASequenceCollection() as dna:
        return list(dna.by_user(id))


@router.post(
    "/users", operation_id="createUser", summary="Create a User", tags=[Tags.USER]
)
def create_user(user: User):
    with UserCollection() as users:
        return users.add(user)


@router.post("/users:bulk", operation_id="bulkCreateUsers", summary="Create Users in bulk", tags=[Tags.USER])
def bulk_create_users(user_list: List[User]):
    with UserCollection() as users:
        return users.update(user_list)
