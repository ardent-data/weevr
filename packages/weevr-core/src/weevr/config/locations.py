"""Filesystem-agnostic config location abstraction.

Config files in weevr can live on the local filesystem or on a remote
Hadoop-supported URI such as ``abfss://``. The :mod:`pathlib` API only
supports the local case, so this module introduces :class:`ConfigLocation`,
a small abstraction with two implementations:

- :class:`LocalConfigLocation` wraps a :class:`pathlib.Path` and delegates to
  the local filesystem.
- :class:`RemoteConfigLocation` holds a URI string and an active
  :class:`SparkSession` and routes operations through the JVM Hadoop
  ``FileSystem`` API.

Callers should obtain a :class:`ConfigLocation` from :func:`make_location`
and pass it through the config-loading pipeline.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class ConfigLocation(ABC):
    """Abstract reference to a config file or directory.

    Implementations encapsulate a single addressing scheme (local filesystem
    or remote Hadoop URI) and expose the minimal surface area the config
    pipeline needs: joining, existence checks, text reads, name and parent
    derivation, and a containment check used for path-traversal protection.
    """

    @abstractmethod
    def join(self, rel: str) -> ConfigLocation:
        """Resolve ``rel`` against this location and return a new location.

        ``rel`` must be a relative path. Implementations normalize ``..``
        and ``.`` segments and reject inputs that look absolute.
        """

    @abstractmethod
    def exists(self) -> bool:
        """Return whether the underlying file or directory exists."""

    @abstractmethod
    def read_text(self) -> str:
        """Return the file contents decoded as UTF-8.

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: For any other I/O failure.
        """

    @abstractmethod
    def is_relative_to(self, other: ConfigLocation) -> bool:
        """Return whether this location is contained within ``other``."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The final path segment, including any extension."""

    @property
    @abstractmethod
    def stem(self) -> str:
        """The final path segment without its extension."""

    @property
    @abstractmethod
    def suffix(self) -> str:
        """The file extension including the leading dot, or empty string."""

    @property
    @abstractmethod
    def parent(self) -> ConfigLocation:
        """The location one level up."""

    @abstractmethod
    def __str__(self) -> str:
        """Return the canonical path or URI for diagnostics and logging."""

    def __fspath__(self) -> str:
        """Return the string form for ``os.fspath`` consumers.

        Remote implementations return their URI; using the result with the
        local filesystem will fail loudly, which is the desired behavior.
        """
        return str(self)


# ---------------------------------------------------------------------------
# Local implementation
# ---------------------------------------------------------------------------


class LocalConfigLocation(ConfigLocation):
    """A :class:`ConfigLocation` backed by a local filesystem path."""

    def __init__(self, path: Path) -> None:
        """Wrap an absolute or relative :class:`Path`.

        Args:
            path: The path to wrap. Stored as-is; resolution and existence
                checks are performed lazily by individual methods.
        """
        self._path = path

    @property
    def path(self) -> Path:
        """The underlying :class:`pathlib.Path`."""
        return self._path

    def join(self, rel: str) -> ConfigLocation:
        """Join ``rel`` to the underlying path.

        Args:
            rel: Relative path to append.

        Returns:
            A new :class:`LocalConfigLocation`.

        Raises:
            ValueError: If ``rel`` is an absolute path.
        """
        if Path(rel).is_absolute():
            raise ValueError(f"Cannot join absolute path '{rel}' to a config location")
        return LocalConfigLocation(self._path / rel)

    def exists(self) -> bool:
        """Return whether the path exists on the local filesystem."""
        return self._path.exists()

    def read_text(self) -> str:
        """Read the file as UTF-8 text."""
        return self._path.read_text(encoding="utf-8")

    def is_relative_to(self, other: ConfigLocation) -> bool:
        """Return whether this path is contained within ``other``.

        Both sides are resolved to absolute paths before comparison so
        relative segments such as ``..`` are honored. Comparing across
        location types always returns ``False``.
        """
        if not isinstance(other, LocalConfigLocation):
            return False
        try:
            return self._path.resolve().is_relative_to(other._path.resolve())
        except (OSError, ValueError):
            return False

    @property
    def name(self) -> str:
        """The final path segment."""
        return self._path.name

    @property
    def stem(self) -> str:
        """The final path segment without its extension."""
        return self._path.stem

    @property
    def suffix(self) -> str:
        """The file extension including the leading dot."""
        return self._path.suffix

    @property
    def parent(self) -> ConfigLocation:
        """The parent directory as a :class:`LocalConfigLocation`."""
        return LocalConfigLocation(self._path.parent)

    def __str__(self) -> str:
        """Return the underlying path as a string."""
        return str(self._path)

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"LocalConfigLocation({self._path!r})"

    def __eq__(self, other: object) -> bool:
        """Two locations are equal when their underlying paths are equal."""
        if not isinstance(other, LocalConfigLocation):
            return NotImplemented
        return self._path == other._path

    def __hash__(self) -> int:
        """Hash by underlying path."""
        return hash(("local", self._path))


# ---------------------------------------------------------------------------
# Remote implementation
# ---------------------------------------------------------------------------


def _split_scheme(uri: str) -> tuple[str, str]:
    """Split ``scheme://rest`` into ``("scheme://", "rest")``.

    Args:
        uri: A URI containing ``://``.

    Returns:
        Tuple of the scheme prefix (including ``://``) and the remainder.

    Raises:
        ValueError: If ``uri`` does not contain ``://``.
    """
    idx = uri.find("://")
    if idx == -1:
        raise ValueError(f"Not a URI: {uri!r}")
    return uri[: idx + 3], uri[idx + 3 :]


def _normalize_uri_path(rest: str) -> str:
    """Normalize the path portion of a URI by resolving ``.`` and ``..``.

    The first ``/``-delimited segment of an ABFS URI is the
    ``container@host`` authority and must not be touched. The function
    therefore preserves the first segment verbatim and only normalizes the
    segments that follow.

    Args:
        rest: The portion of a URI after ``scheme://``.

    Returns:
        The normalized remainder.

    Raises:
        ValueError: If ``..`` walks above the authority root.
    """
    if not rest:
        return rest

    # Capture trailing-slash intent so a directory-style URI like
    # "ws@host/lh/Files/" survives normalization. The flag is dropped if
    # normalization collapses everything down to the bare authority — at
    # the authority root the trailing slash is meaningless.
    trailing = rest.endswith("/")
    parts = rest.split("/")

    # First segment is the authority (e.g. container@host); never normalize.
    authority = parts[0]
    segments = parts[1:]

    out: list[str] = []
    for seg in segments:
        if seg == "" or seg == ".":
            continue
        if seg == "..":
            if not out:
                raise ValueError(f"Path traversal escapes URI authority: {rest!r}")
            out.pop()
            continue
        out.append(seg)

    normalized = authority + ("/" + "/".join(out) if out else "")
    if trailing and out:
        normalized += "/"
    return normalized


class RemoteConfigLocation(ConfigLocation):
    """A :class:`ConfigLocation` backed by a Hadoop-accessible URI.

    Operations route through the JVM ``org.apache.hadoop.fs.FileSystem`` API
    available on the active :class:`SparkSession`. The implementation works
    for any scheme that Spark's Hadoop client can resolve, including
    ``abfss://``, ``wasbs://``, ``s3a://``, ``gs://``, and ``file://``.
    """

    def __init__(self, uri: str, spark: SparkSession) -> None:
        """Construct a remote location.

        Args:
            uri: A fully qualified URI such as
                ``abfss://workspace@onelake.dfs.fabric.microsoft.com/lh/Files/proj``.
            spark: Active :class:`SparkSession`. Used only to access
                ``_jvm`` and ``_jsc``; the session is not stored in any
                long-lived state outside this object.

        Raises:
            ValueError: If ``uri`` does not contain ``://``.
        """
        if "://" not in uri:
            raise ValueError(f"RemoteConfigLocation requires a URI, got: {uri!r}")
        self._uri = uri
        self._spark = spark

    @property
    def uri(self) -> str:
        """The underlying URI string."""
        return self._uri

    # -- JVM helpers --------------------------------------------------------

    def _jvm(self) -> Any:
        """Return the active ``spark._jvm`` handle (Py4J dynamic proxy)."""
        return self._spark._jvm  # type: ignore[attr-defined]

    def _hadoop_path(self) -> Any:
        """Construct a JVM ``org.apache.hadoop.fs.Path`` for this URI."""
        jvm = self._jvm()
        return jvm.org.apache.hadoop.fs.Path(self._uri)

    def _filesystem(self) -> Any:
        """Resolve the JVM ``FileSystem`` for this URI."""
        hpath = self._hadoop_path()
        hadoop_conf = self._spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        return hpath.getFileSystem(hadoop_conf)

    # -- ConfigLocation surface --------------------------------------------

    def join(self, rel: str) -> ConfigLocation:
        """Join ``rel`` to this URI, normalizing ``.`` and ``..`` segments.

        Args:
            rel: Relative path to append.

        Returns:
            A new :class:`RemoteConfigLocation`.

        Raises:
            ValueError: If ``rel`` is absolute, contains its own scheme, or
                walks above the URI authority via ``..``.
        """
        if "://" in rel:
            raise ValueError(f"Cannot join absolute URI '{rel}' to a config location")
        if rel.startswith("/"):
            raise ValueError(f"Cannot join absolute path '{rel}' to a config location")

        scheme, rest = _split_scheme(self._uri)
        joined = rest.rstrip("/") + "/" + rel.lstrip("/")
        normalized = _normalize_uri_path(joined)
        return RemoteConfigLocation(scheme + normalized, self._spark)

    def exists(self) -> bool:
        """Return whether the underlying URI exists.

        Wraps ``FileSystem.exists``. JVM exceptions are propagated as
        :class:`OSError` so callers can handle filesystem errors uniformly.
        """
        try:
            return bool(self._filesystem().exists(self._hadoop_path()))
        except Exception as exc:  # Py4JJavaError or similar
            raise OSError(f"Failed to check existence of {self._uri}: {exc}") from exc

    def read_text(self) -> str:
        """Read the file as UTF-8 text via Hadoop ``FileSystem.open``.

        Raises:
            FileNotFoundError: If the underlying file does not exist.
            OSError: For any other I/O failure.
        """
        jvm = self._jvm()
        try:
            fs = self._filesystem()
            hpath = self._hadoop_path()
            if not fs.exists(hpath):
                raise FileNotFoundError(self._uri)
            stream = fs.open(hpath)
        except FileNotFoundError:
            raise
        except Exception as exc:  # Py4JJavaError, etc.
            msg = str(exc)
            if "FileNotFoundException" in msg or "does not exist" in msg.lower():
                raise FileNotFoundError(self._uri) from exc
            raise OSError(f"Failed to open {self._uri}: {exc}") from exc

        try:
            try:
                bytes_obj = jvm.org.apache.commons.io.IOUtils.toByteArray(stream)
            except Exception as exc:  # commons-io missing or read failed
                raise OSError(f"Failed to read {self._uri}: {exc}") from exc
        finally:
            with contextlib.suppress(Exception):
                stream.close()

        return bytes(bytes_obj).decode("utf-8")

    def is_relative_to(self, other: ConfigLocation) -> bool:
        """Return whether this URI lies within ``other``.

        Comparison is a normalized prefix match on the URI string. Comparing
        across location types always returns ``False``.
        """
        if not isinstance(other, RemoteConfigLocation):
            return False
        self_scheme, self_rest = _split_scheme(self._uri)
        other_scheme, other_rest = _split_scheme(other._uri)
        if self_scheme != other_scheme:
            return False
        try:
            self_norm = _normalize_uri_path(self_rest)
            other_norm = _normalize_uri_path(other_rest)
        except ValueError:
            return False
        if self_norm == other_norm:
            return True
        return self_norm.startswith(other_norm.rstrip("/") + "/")

    @property
    def name(self) -> str:
        """The final ``/``-delimited segment of the URI."""
        _, rest = _split_scheme(self._uri)
        return rest.rstrip("/").rsplit("/", 1)[-1]

    @property
    def stem(self) -> str:
        """The final segment with its extension stripped."""
        n = self.name
        dot = n.rfind(".")
        return n if dot <= 0 else n[:dot]

    @property
    def suffix(self) -> str:
        """The file extension including the leading dot."""
        n = self.name
        dot = n.rfind(".")
        return "" if dot <= 0 else n[dot:]

    @property
    def parent(self) -> ConfigLocation:
        """The URI one level up.

        Returns the same location when already at the authority root.
        """
        scheme, rest = _split_scheme(self._uri)
        rest = rest.rstrip("/")
        if "/" not in rest:
            return RemoteConfigLocation(self._uri, self._spark)
        parent_rest = rest.rsplit("/", 1)[0]
        return RemoteConfigLocation(scheme + parent_rest, self._spark)

    def __str__(self) -> str:
        """Return the underlying URI."""
        return self._uri

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"RemoteConfigLocation({self._uri!r})"

    def __eq__(self, other: object) -> bool:
        """Two remote locations are equal when their URIs match."""
        if not isinstance(other, RemoteConfigLocation):
            return NotImplemented
        return self._uri == other._uri

    def __hash__(self) -> int:
        """Hash by URI."""
        return hash(("remote", self._uri))


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_location(
    path_or_uri: str | Path | ConfigLocation,
    spark: SparkSession | None = None,
) -> ConfigLocation:
    """Construct a :class:`ConfigLocation` from a path, URI, or existing location.

    A string containing ``://`` is treated as a remote URI and requires a
    ``spark`` session. Anything else is treated as a local filesystem path.
    Existing :class:`ConfigLocation` inputs are returned unchanged.

    Args:
        path_or_uri: A local path, a URI string, or an existing
            :class:`ConfigLocation`.
        spark: Active :class:`SparkSession`. Required when ``path_or_uri``
            is a remote URI.

    Returns:
        A :class:`ConfigLocation` instance.

    Raises:
        ValueError: If a remote URI is supplied without a ``spark`` session.
    """
    if isinstance(path_or_uri, ConfigLocation):
        return path_or_uri

    s = str(path_or_uri)
    if "://" in s:
        if spark is None:
            raise ValueError(f"Remote config location '{s}' requires an active SparkSession")
        return RemoteConfigLocation(s, spark)
    return LocalConfigLocation(Path(s))
