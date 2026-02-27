"""Iceberg REST catalog interaction and S3 data writer."""

from __future__ import annotations

import logging
from typing import Literal

import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
)

from json_to_iceberg.config import Config
from json_to_iceberg.schema import dataframe_to_arrow, resolve_null_types

log = logging.getLogger(__name__)

Mode = Literal["append", "overwrite"]


def get_catalog(cfg: Config) -> RestCatalog:
    """Instantiate a PyIceberg REST catalog from *cfg*."""
    catalog = load_catalog("rest", **cfg.catalog_properties())
    log.info("Connected to Iceberg REST catalog at %s", cfg.catalog_uri)
    return catalog  # type: ignore[return-value]


def ensure_namespace(catalog: RestCatalog, namespace: str) -> None:
    """Create *namespace* if it does not already exist."""
    try:
        catalog.create_namespace(namespace)
        log.info("Created namespace '%s'", namespace)
    except NamespaceAlreadyExistsError:
        log.debug("Namespace '%s' already exists", namespace)


def write_to_iceberg(
    cfg: Config,
    df: pl.DataFrame,
    *,
    mode: Mode = "overwrite",
) -> None:
    """End-to-end: resolve schema → connect to catalog → write to Iceberg.

    Args:
        cfg: Fully-populated configuration.
        df: Polars DataFrame (may contain ``Null``-typed columns).
        mode: ``"append"`` or ``"overwrite"``.
    """
    # 1. Resolve nulls and convert to Arrow
    df = resolve_null_types(df)
    arrow_table = dataframe_to_arrow(df)
    log.info(
        "Resolved schema with %d columns, %d rows",
        len(arrow_table.schema),
        arrow_table.num_rows,
    )

    # 2. Catalog + namespace
    catalog = get_catalog(cfg)
    ensure_namespace(catalog, cfg.namespace)

    table_id = f"{cfg.namespace}.{cfg.table_name}"

    # 3. Create or load the table
    try:
        table = catalog.load_table(table_id)
        log.info("Loaded existing table '%s'", table_id)
    except NoSuchTableError:
        table = catalog.create_table(table_id, schema=arrow_table.schema)
        log.info("Created new table '%s'", table_id)

    # 4. Write data
    if mode == "overwrite":
        table.overwrite(arrow_table)
        log.info("Overwrote table '%s' with %d rows", table_id, arrow_table.num_rows)
    else:
        table.append(arrow_table)
        log.info("Appended %d rows to table '%s'", arrow_table.num_rows, table_id)
