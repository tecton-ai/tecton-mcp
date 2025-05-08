import json, os, shutil
from tecton_mcp.tecton_utils import APIGraphBuilder
from tecton_mcp.tools.example_code_snippet_tools import build_and_save_example_code_snippet_index
from tecton_mcp.embed.meta import write_metadata, DATA_DIR
from tecton_mcp.constants import FILE_DIR
import tecton
import pandas as pd

EMBED_MODEL = "sentence-transformers/multi-qa-MiniLM-L6-cos-v1"


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    examples = pd.read_parquet(os.path.join(FILE_DIR, "data", "examples.parquet")).to_dict(orient="records")

    build_and_save_example_code_snippet_index(examples, EMBED_MODEL)
    write_metadata(EMBED_MODEL, tecton.__version__)
    print("Embeddings regenerated and metadata saved at", DATA_DIR)


if __name__ == "__main__":
    main() 