from pathlib import Path


def list_partitions(silver_transactions_path: Path) -> list[str]:
    return sorted([p.name for p in silver_transactions_path.glob("*.parquet")])


def normalize_requested_partition(name: str) -> str:
    name = name.strip()
    if not name.endswith(".parquet"):
        name = f"{name}.parquet"
    return name


def validate_partitions(
    requested_partitions: list[str],
    available_partitions: list[str]
) -> list[str]:
    normalized_requested = [
        normalize_requested_partition(p) for p in requested_partitions
    ]

    missing = [p for p in normalized_requested if p not in available_partitions]
    if missing:
        raise ValueError(
            "Estas particiones no existen en silver/transactions: "
            f"{missing}\n"
            f"Disponibles: {available_partitions}"
        )

    return normalized_requested


def resolve_backfill_partitions(
    silver_transactions_path: Path,
    partitions: list[str] | None = None
) -> list[str]:
    available = list_partitions(silver_transactions_path)

    if not available:
        raise ValueError("No se encontraron particiones parquet en silver/transactions")

    if partitions is None or len(partitions) == 0:
        return available

    return validate_partitions(partitions, available)