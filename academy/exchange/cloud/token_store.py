from __future__ import annotations

import pathlib
import threading
from collections.abc import Mapping

from globus_sdk.tokenstorage import JSONTokenStorage
from globus_sdk.tokenstorage import TokenStorageData


class LockingTokenStorage(JSONTokenStorage):
    """A thread safe Globus token store.

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
        self._lock = threading.Lock()
        super().__init__(filepath, namespace=namespace)

    def store_token_data_by_resource_server(
        self,
        token_data_by_resource_server: Mapping[str, TokenStorageData],
    ) -> None:
        """Store token data for resource server(s) in the current namespace.

        Args:
            token_data_by_resource_server: mapping of resource server to
                token data.
        """
        with self._lock:
            return super().store_token_data_by_resource_server(
                token_data_by_resource_server,
            )

    def get_token_data_by_resource_server(self) -> dict[str, TokenStorageData]:
        """Retrieve all token data stored in the current namespace.

        Returns:
            a dict of ``TokenStorageData`` objects indexed by their
                resource server.
        """
        with self._lock:
            return super().get_token_data_by_resource_server()

    def remove_token_data(self, resource_server: str) -> bool:
        """Remove token data for a resource server in the current namespace.

        Args:
            resource_server: The resource server to remove token data for.

        Returns:
            True if token data was deleted, False if none was found to delete.
        """
        with self._lock:
            return super().remove_token_data(resource_server)
