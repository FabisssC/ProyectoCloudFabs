import json
from pathlib import Path

STATE_FILE = Path("gold/_state/processed_partitions.json")


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_state(processed):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2)


def reset_state():
    save_state([])


def get_new_partitions(silver_path: Path):
    processed = load_state()
    all_parts = sorted([p.name for p in silver_path.glob("*.parquet")])
    new_parts = [p for p in all_parts if p not in processed]
    return new_parts, processed


def update_state(processed, new_parts):
    updated = sorted(set(processed + new_parts))
    save_state(updated)