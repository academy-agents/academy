from __future__ import annotations

import pathlib
import threading
from collections.abc import Mapping

from globus_sdk.tokenstorage import SQLiteTokenStorage
from globus_sdk.tokenstorage import TokenStorage
from globus_sdk.tokenstorage import TokenStorageData


class SafeSQLiteTokenStorage(TokenStorage):
    """A thread safe Globus SQLite token store.

    Args:
        filepath: The path to a file where token data should be stored.
        namespace: A unique string for partitioning token data
            (Default: "DEFAULT").
    """

    def __init__(
        self,
        filepath: pathlib.Path | str,
        *,
        namespace: str = 'DEFAULT',
    ) -> None:
        super().__init__(namespace=namespace)
        self._local_data = threading.local()
        self.filepath = filepath
        self.namespace = namespace

    @property
    def token_store(self) -> SQLiteTokenStorage:
        """Internal thread local token storage."""
        try:
            return self._local_data.token_store
        except AttributeError:
            self._local_data.token_store = SQLiteTokenStorage(
                filepath=self.filepath,
                namespace=self.namespace,
            )
            return self._local_data.token_store

    def store_token_data_by_resource_server(
        self,
        token_data_by_resource_server: Mapping[str, TokenStorageData],
    ) -> None:
        """Store token data for resource server(s) in the current namespace.

        Args:
            token_data_by_resource_server: mapping of resource server to
                token data.
        """
        return self.token_store.store_token_data_by_resource_server(
            token_data_by_resource_server,
        )

    def get_token_data_by_resource_server(self) -> dict[str, TokenStorageData]:
        """Retrieve all token data stored in the current namespace.

        Returns:
            a dict of ``TokenStorageData`` objects indexed by their
                resource server.
        """
        return self.token_store.get_token_data_by_resource_server()

    def remove_token_data(self, resource_server: str) -> bool:
        """Remove token data for a resource server in the current namespace.

        Args:
            resource_server: The resource server to remove token data for.

        Returns:
            True if token data was deleted, False if none was found to delete.
        """
        return self.token_store.remove_token_data(resource_server)
