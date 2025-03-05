from pydantic import PostgresDsn, SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Environment settings."""

    PGUSER: str
    PGPASSWORD: SecretStr
    PGHOST: str
    PGPORT: int = 5432
    PGDATABASE: str

    @property
    def db_url(self) -> str:
        """PostgreSQL connection URL."""
        return str(
            PostgresDsn.build(
                scheme="postgres",
                username=self.PGUSER,
                password=self.PGPASSWORD.get_secret_value(),
                host=self.PGHOST,
                port=self.PGPORT,
                path=self.PGDATABASE,
            )
        )
