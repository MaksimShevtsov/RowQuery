from contextlib import contextmanager
import importlib
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any


class ConnectionConfig(BaseModel):
    """Configuration for database connections."""
    driver: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: str
    pool_size: int = 5
    pool_timeout: int = 30
    pool_recycle: int = 1800
    extra: Dict[str, Any] = {}


class ConnectionManager:
    """
    Handle database connections and pooling.
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._pool = None
        self._adapter = self._load_adapter()

    def _load_adapter(self):
        try:
            module_path = f"row_query.adapters.{self.config.driver.lower()}"
            module = importlib.import_module(module_path)
            return module.DBAdapter(self.config)
        except (ImportError, AttributeError) as e:
            raise ValueError(
                f"Unsupported database driver: {self.config.driver}: {e}"
            ) from e
        
    def initialize_pool(self):
        """
        Initialize the connection pool.
        """
        if self._pool is None:
            self._pool = self._adapter.create_pool()
        return self._pool
    

    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.
        """
        if self._pool is None:
            self.initialize_pool()
        connection = self._adapter.acquire_connection(self._pool)
        try:
            yield connection
        finally:
            self._adapter.release_connection(connection, self._pool)

    
    def close_pool(self):
        """
        Close the connection pool.
        """
        if self._pool:
            self._adapter.close_pool(self._pool)
            self._pool = None
