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
    
    # Determine which database to use based on current workspace compute mode
    try:
        import tecton
        compute_mode = tecton.conf.get_or_none('TECTON_BATCH_COMPUTE_MODE')
        db_name = (
            "tecton_examples_spark.db" 
            if compute_mode and compute_mode.lower() == 'spark' 
            else "tecton_examples_rift.db"
        )
    except Exception:
        # If tecton import fails or no workspace connected, default to Rift
        db_name = "tecton_examples_rift.db"
    
    spark_path = os.path.join(FILE_DIR, "data", "tecton_examples_spark.db")
    if not os.path.exists(spark_path):
        raise FileNotFoundError(f"Example Spark embeddings db not found: {spark_path}. Run generate_embeddings.py")

    rift_path = os.path.join(FILE_DIR, "data", "tecton_examples_rift.db")
    if not os.path.exists(rift_path):
        raise FileNotFoundError(f"Example Rift embeddings db not found: {rift_path}. Run generate_embeddings.py")

    spark_db = VectorDB("lancedb", uri=spark_path, embedding=get_embedding_model())
    rift_db = VectorDB("lancedb", uri=rift_path, embedding=get_embedding_model())

    @vdb.retriever(name="tecton_examples", top_k=10)
    def tecton_examples_retriever(query, filter, result) -> str:
        code = set(x["code"] for x in result)
        prefix = "==== Python Code Example ====\n\n"
        return "\n\n".join([prefix + c for c in code])

    return tecton_examples_retriever

def build_and_save_example_code_snippet_index(examples, embedding_model: str, db_name: str):
    """Build index from examples list and save to disk with custom database name."""
    vpath = os.path.join(FILE_DIR, "data", db_name)
    shutil.rmtree(vpath, ignore_errors=True)
    vdb = VectorDB("lancedb", uri=vpath, embedding=embedding_model)
    vdb.ingest(
        texts=[obj["text"] for obj in examples],
        metadatas=[dict(code=obj["code"], title=obj["text"]) for obj in examples],
    ) 