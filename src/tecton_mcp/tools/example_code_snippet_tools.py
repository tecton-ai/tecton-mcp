"""
Tools for building and managing the example code snippets index.
This module provides functionality to build and query the Tecton code examples.
"""

import os
import shutil
import pandas as pd
from functools import lru_cache

from tecton_mcp.constants import FILE_DIR
from tecton_mcp.embed import VectorDB
from tecton_mcp.embed.meta import get_embedding_model


def load_example_code_snippet_index():
    """Load the example index from disk and return a retriever."""
    vpath = os.path.join(FILE_DIR, "data", "tecton_examples.db")
    if not os.path.exists(vpath):
        raise FileNotFoundError("Example embeddings db not found; run generate_embeddings.py")

    vdb = VectorDB("lancedb", uri=vpath, embedding=get_embedding_model())

    @vdb.retriever(name="tecton_examples", top_k=10)
    def tecton_examples_retriever(query, filter, result) -> str:
        code = set(x["code"] for x in result)
        prefix = "==== Python Code Example ====\n\n"
        return "\n\n".join([prefix + c for c in code])

    return tecton_examples_retriever


def build_and_save_example_code_snippet_index(examples, embedding_model: str):
    """Build index from examples list and save to disk."""
    vpath = os.path.join(FILE_DIR, "data", "tecton_examples.db")
    shutil.rmtree(vpath, ignore_errors=True)
    vdb = VectorDB("lancedb", uri=vpath, embedding=embedding_model)
    vdb.ingest(
        texts=[obj["text"] for obj in examples],
        metadatas=[dict(code=obj["code"], title=obj["text"]) for obj in examples],
    )

    # remove bottom old retriever definition (we replaced above) 