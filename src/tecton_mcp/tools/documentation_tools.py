import os
import shutil
import re
from typing import List, Dict, Any, Optional
from tecton_mcp.embed import VectorDB
from tecton_mcp.constants import FILE_DIR
from tecton_mcp.embed.meta import get_embedding_model

# ---------------------------------------------------------------------------
# Dynamically resolve the correct documentation DB based on installed Tecton
# version. Multiple DB files (e.g. `tecton_docs_1.1.db`, `tecton_docs_1.0.db`) 
# may coexist under `{FILE_DIR}/data`.  The mapping rules are:
#   - Version > 1.1.x  -> `tecton_docs.db`   (default / latest)
#   - Version 1.1.x    -> `tecton_docs_1.1.db`
#   - Version 1.0.x -> `tecton_docs_1.0.db`
#   - Anything else / unknown -> fall back to the default (`tecton_docs.db`).
# ---------------------------------------------------------------------------

def _get_installed_tecton_version() -> Optional[str]:
    """Return the installed Tecton version string or ``None`` if not installed."""
    try:
        import tecton  # noqa: F401  # Local import to avoid hard dependency when not needed
        version = getattr(tecton, "__version__", None)
        return version
    except ImportError:
        print("Tecton is not installed. Cannot determine Tecton version.")
        return None

def _resolve_docs_db_path() -> str:
    """Determine the correct documentation DB path based on Tecton version."""
    version = _get_installed_tecton_version()

    # Default filename (latest documentation)
    db_filename = "tecton_docs.db"

    if version:
        # Extract major and minor numbers (digits) from the version string.
        # Handles versions like `1.1.0`, `1.2.0b12`, `1.0.3rc1`, etc.
        match = re.match(r"^(\d+)\.(\d+)", version)
        if match:
            major, minor = int(match.group(1)), int(match.group(2))

            if major == 1 and minor == 1:
                db_filename = "tecton_docs_1.1.db"
            elif (major == 1 and minor == 0):
                db_filename = "tecton_docs_1.0.db"
            # Versions >1.1 keep the default, which points to the latest docs.

    resolved_path = os.path.join(FILE_DIR, "data", db_filename)
    return resolved_path

# Centralized path for the documentation LanceDB database
DOCS_DB_PATH = _resolve_docs_db_path()

def build_and_save_documentation_index(
    documentation_chunks: List[Dict[str, Any]], 
    embedding_model_name: str,
    db_path: str,
):
    """
    Builds a LanceDB index from processed documentation chunks and saves it to disk.
    The database is saved at the path defined by DOCS_DB_PATH.

    Args:
        documentation_chunks: A list of dictionaries, where each dictionary represents a 
                              documentation chunk and contains keys like 'text_chunk', 'url', 
                              'source_file', 'header', 'word_length'.
        embedding_model_name: The name of the embedding model to use.
        db_path: Destination path for the LanceDB file to create (e.g.,
                 `~/.../tecton_docs_1.0.db`).
    """

    if os.path.exists(db_path):
        print(f'Removing existing documentation embeddings database: {db_path}')
        shutil.rmtree(db_path)
    
    vdb = VectorDB("lancedb", uri=db_path, embedding=embedding_model_name)
    
    if not documentation_chunks:
        print('No documentation chunks provided. Skipping LanceDB index creation.')
        return

    texts_for_lancedb = [chunk["text_chunk"] for chunk in documentation_chunks]
    
    # Prepare metadatas for LanceDB.
    # We will store url, header, and the text_chunk itself.
    all_metadatas_for_lancedb = [
        {
            "url": chunk.get("url", "N/A"), 
            "header": chunk.get("header", "N/A"), 
            "text_chunk": chunk.get("text_chunk", "") 
        } 
        for chunk in documentation_chunks
    ]

    print(f'Ingesting {len(texts_for_lancedb)} text chunks into LanceDB at: {db_path}')
    vdb.ingest(
        texts=texts_for_lancedb,
        metadatas=all_metadatas_for_lancedb,
    )
    print(f'Documentation embeddings successfully saved to: {db_path}')

def load_documentation_index():
    """Load the Tecton documentation LanceDB index from disk and return a retriever."""
    if not os.path.exists(DOCS_DB_PATH):
        raise FileNotFoundError(
            f"Documentation embeddings database not found at {DOCS_DB_PATH}. "
            "Please run the `src/tecton_mcp/scripts/generate_embeddings.py` script first."
        )

    # Use get_embedding_model() to ensure consistency with how the db was created
    embedding_model_name = get_embedding_model()

    vdb = VectorDB("lancedb", uri=DOCS_DB_PATH, embedding=embedding_model_name)

    @vdb.retriever(name="tecton_documentation", top_k=10)
    def tecton_documentation_retriever(query: str, filter: Any, result: List[Dict[str, Any]]) -> str:
        """
        Retrieves and formats Tecton documentation snippets based on a query.
        Each snippet includes the TECTON DOCUMENTATION URL (Source URL), 
        the section header, and the relevant text chunk.
        """
        formatted_results = []
        for item in result:
            # Metadata keys are 'url', 'header', 'text_chunk' as defined during ingestion
            url = item.get("url", "N/A")
            header = item.get("header", "N/A")
            text_chunk = item.get("text_chunk", "No content found.")
            
            prefix = "==== Documentation Snippet ===="
            formatted_item = f"{prefix}\nSource URL: {url}\nSection Header: {header}\n\n{text_chunk}"
            formatted_results.append(formatted_item)
        
        return "\n\n---\n\n".join(formatted_results)

    return tecton_documentation_retriever 