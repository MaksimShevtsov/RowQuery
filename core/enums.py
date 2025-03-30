from enum import Enum


class DBAdapter(Enum):
    """
    Enum for database adapters.
    """
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    