"""Key management models."""

from typing import Literal

from weevr.model.base import FrozenBase


class SurrogateKeyConfig(FrozenBase):
    """Configuration for surrogate key generation.

    Attributes:
        name: Output column name for the generated surrogate key.
        algorithm: Hash algorithm to use. ``crc32`` has a small 32-bit output
            space with higher collision risk. ``murmur3`` (Spark's ``hash()``)
            may produce different results across Spark major versions. Neither
            is recommended for high-cardinality surrogate key use cases.
        output: Controls the output type for integer-returning algorithms
            (xxhash64, crc32, murmur3). ``native`` preserves the algorithm's
            return type; ``string`` casts to StringType. Has no practical
            effect on sha*/md5 which already return hex strings.
    """

    name: str
    algorithm: Literal[
        "xxhash64", "sha1", "sha256", "sha384", "sha512", "md5", "crc32", "murmur3"
    ] = "sha256"
    output: Literal["native", "string"] = "native"


class ChangeDetectionConfig(FrozenBase):
    """Configuration for change detection hash generation.

    Attributes:
        name: Output column name for the generated hash.
        columns: Columns to include in the change detection hash.
        algorithm: Hash algorithm to use. ``crc32`` has a small 32-bit output
            space with higher collision risk. ``murmur3`` (Spark's ``hash()``)
            may produce different results across Spark major versions. Neither
            is recommended for high-cardinality change detection use cases.
        output: Controls the output type for integer-returning algorithms
            (xxhash64, crc32, murmur3). ``native`` preserves the algorithm's
            return type; ``string`` casts to StringType. Has no practical
            effect on sha*/md5 which already return hex strings.
    """

    name: str
    columns: list[str]
    algorithm: Literal[
        "xxhash64", "sha1", "sha256", "sha384", "sha512", "md5", "crc32", "murmur3"
    ] = "md5"
    output: Literal["native", "string"] = "native"


class KeyConfig(FrozenBase):
    """Key management configuration for a thread."""

    business_key: list[str] | None = None
    surrogate_key: SurrogateKeyConfig | None = None
    change_detection: ChangeDetectionConfig | None = None
