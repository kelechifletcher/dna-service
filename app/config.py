from pydantic import BaseSettings


class Config(BaseSettings):
    db_host: str = "0.0.0.0"
    db_port: str = "5432"
    db_username: str = "postgres"
    db_password: str = "dna"
