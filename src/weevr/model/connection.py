"""Connection declaration models."""

from typing import Literal

from pydantic import Field

from weevr.model.base import FrozenBase


class OneLakeConnection(FrozenBase):
    """A OneLake connection declaration.

    Identifies the Fabric workspace and lakehouse that a source or target
    resolves against at execution time.
    """

    type: Literal["onelake"] = Field(
        description="Connection type identifier.",
    )
    workspace: str = Field(
        description="OneLake workspace GUID or variable reference.",
    )
    lakehouse: str = Field(
        description="OneLake lakehouse GUID or variable reference.",
    )
    default_schema: str | None = Field(
        default=None,
        description="Default schema for tables in this connection.",
    )
