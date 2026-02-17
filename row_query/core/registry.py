"""SQL Registry - loads and caches SQL files from a directory structure.

Namespace convention:
    sql/user/get_by_id.sql     -> "user.get_by_id"
    sql/billing/invoice/list.sql -> "billing.invoice.list"
"""

from __future__ import annotations

from pathlib import Path

from row_query.core.exceptions import DuplicateQueryError, QueryNotFoundError


class SQLRegistry:
    """Loads and caches SQL files from a directory structure.

    The registry is immutable after loading: load once at startup, then
    read-only access for the lifetime of the application.

    Args:
        root_dir: Root directory containing SQL files.

    Raises:
        DuplicateQueryError: If two files resolve to the same namespace key.
    """

    def __init__(self, root_dir: Path | str) -> None:
        self._root_dir = Path(root_dir)
        self._queries: dict[str, str] = {}
        self._query_paths: dict[str, Path] = {}  # Track file paths
        self._load()

    def _load(self) -> None:
        """Recursively load all .sql files from root directory."""
        if not self._root_dir.exists():
            return

        for sql_file in sorted(self._root_dir.rglob("*.sql")):
            relative = sql_file.relative_to(self._root_dir)
            # Build namespace: remove .sql extension, replace path separators with dots
            parts = list(relative.parts)
            parts[-1] = parts[-1].removesuffix(".sql")
            query_name = ".".join(parts)

            if query_name in self._queries:
                raise DuplicateQueryError(
                    query_name,
                    str(self._query_paths[query_name]),
                    str(sql_file),
                )

            self._queries[query_name] = sql_file.read_text(encoding="utf-8").strip()
            self._query_paths[query_name] = sql_file

    def get(self, query_name: str) -> str:
        """Look up SQL text by namespace-qualified name.

        Args:
            query_name: Dot-separated query name (e.g., "user.get_by_id").

        Returns:
            The SQL text content of the file.

        Raises:
            QueryNotFoundError: If no query matches the given name.
        """
        try:
            return self._queries[query_name]
        except KeyError:
            raise QueryNotFoundError(query_name) from None

    def has(self, query_name: str) -> bool:
        """Check if a query name is registered."""
        return query_name in self._queries

    @property
    def query_names(self) -> list[str]:
        """List all registered query names, sorted alphabetically."""
        return sorted(self._queries.keys())

    def __len__(self) -> int:
        """Number of registered queries."""
        return len(self._queries)
