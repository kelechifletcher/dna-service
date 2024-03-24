from fastapi import FastAPI

from app.context import Context
from app.services.db import DBService
from app.routers.dna import router as dna_router
from app.routers.user import router as user_router

app = FastAPI()
context = Context()


@app.on_event("startup")
def init_db():
    """
    Initializes database service (singleton) upon application start-up.
    """
    context.db = DBService()


@app.on_event("startup")
def create_all():
    """
    Creates all tables defined by database service upon application start-up.
    """
    context.db.create_all()

    """Adds pg_trgm extension for optimized infix pattern matching."""
    context.db.add_pg_trgm()


@app.on_event("shutdown")
def close_db():
    """
    Closes and disposes of all database connections upon application shutdown.
    """
    context.db.exit()


app.include_router(dna_router)
app.include_router(user_router)
