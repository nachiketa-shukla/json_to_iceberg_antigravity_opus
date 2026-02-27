"""CLI entrypoint for json-to-iceberg."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from json_to_iceberg.config import Config
from json_to_iceberg.flatten import load_and_flatten
from json_to_iceberg.writer import write_to_iceberg


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="json-to-iceberg",
        description="Ingest nested JSON into an Apache Iceberg table.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Flatten JSON and write to Iceberg")
    ingest.add_argument(
        "-i",
        "--input",
        required=True,
        type=Path,
        help="Path to the JSON file",
    )
    ingest.add_argument(
        "-n",
        "--namespace",
        default=None,
        help="Iceberg namespace (overrides ICEBERG_NAMESPACE env var)",
    )
    ingest.add_argument(
        "-t",
        "--table",
        default=None,
        help="Iceberg table name (overrides ICEBERG_TABLE env var)",
    )
    ingest.add_argument(
        "-m",
        "--mode",
        choices=["append", "overwrite"],
        default="overwrite",
        help="Write mode (default: overwrite)",
    )
    ingest.add_argument(
        "--separator",
        default=".",
        help="Separator for flattened key names (default: '.')",
    )
    ingest.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.command == "ingest":
        _ingest(args)


def _ingest(args: argparse.Namespace) -> None:
    """Run the ingest pipeline."""
    log = logging.getLogger("json_to_iceberg")

    # Build config, overriding with CLI flags where provided
    overrides: dict[str, str] = {}
    if args.namespace:
        overrides["namespace"] = args.namespace
    if args.table:
        overrides["table_name"] = args.table
    cfg = Config(**overrides)

    # 1. Load & flatten
    log.info("Loading JSON from %s", args.input)
    df = load_and_flatten(args.input, separator=args.separator)
    log.info("Flattened to %d rows × %d columns", df.height, df.width)
    log.debug("Schema:\n%s", df.schema)

    # 2. Write to Iceberg
    write_to_iceberg(cfg, df, mode=args.mode)
    log.info("Done ✓")


if __name__ == "__main__":
    main()
