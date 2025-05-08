import json
import os
from tecton_mcp.constants import FILE_DIR
from typing import Any, Dict

DATA_DIR = os.path.join(FILE_DIR, "data")
META_PATH = os.path.join(DATA_DIR, "embeddings_meta.json")
DEFAULT_MODEL = "jinaai/jina-embeddings-v2-base-code"


def get_metadata() -> Dict[str, Any]:
    """Return metadata dict or empty dict if not found."""
    try:
        with open(META_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def get_embedding_model() -> str:
    """Return embedding model specified in metadata or default."""
    return get_metadata().get("embedding_model", DEFAULT_MODEL)


def write_metadata(embedding_model: str, tecton_version: str):
    """Write metadata json to disk."""
    os.makedirs(DATA_DIR, exist_ok=True)
    meta = {
        "embedding_model": embedding_model,
        "tecton_version": tecton_version,
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2) 