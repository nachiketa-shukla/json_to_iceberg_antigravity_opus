"""Environment-based configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    """Immutable configuration loaded from environment variables.

    Every setting can also be overridden programmatically.
    """

    # Iceberg REST catalog
    catalog_uri: str = field(
        default_factory=lambda: os.environ.get(
            "ICEBERG_REST_URI", "http://localhost:8181"
        )
    )
    warehouse: str = field(
        default_factory=lambda: os.environ.get(
            "ICEBERG_WAREHOUSE", "s3://warehouse/"
        )
    )

    # S3-compatible object store
    s3_endpoint: str = field(
        default_factory=lambda: os.environ.get(
            "S3_ENDPOINT", "http://localhost:9000"
        )
    )
    s3_access_key: str = field(
        default_factory=lambda: os.environ.get("S3_ACCESS_KEY", "admin")
    )
    s3_secret_key: str = field(
        default_factory=lambda: os.environ.get("S3_SECRET_KEY", "password")
    )
    s3_region: str = field(
        default_factory=lambda: os.environ.get("S3_REGION", "us-east-1")
    )

    # Iceberg table target
    namespace: str = field(
        default_factory=lambda: os.environ.get("ICEBERG_NAMESPACE", "default")
    )
    table_name: str = field(
        default_factory=lambda: os.environ.get("ICEBERG_TABLE", "raw")
    )

    def catalog_properties(self) -> dict[str, str]:
        """Return the properties dict expected by ``RestCatalog``."""
        return {
            "uri": self.catalog_uri,
            "warehouse": self.warehouse,
            "s3.endpoint": self.s3_endpoint,
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "s3.region": self.s3_region,
        }
