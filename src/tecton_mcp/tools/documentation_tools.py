import os
import shutil
from typing import List, Dict, Any
from tecton_mcp.embed import VectorDB
from tecton_mcp.constants import FILE_DIR
from tecton_mcp.embed.meta import get_embedding_model

# Centralized path for the documentation LanceDB database
DOCS_DB_PATH = os.path.join(FILE_DIR, "data", "tecton_docs.db")

def build_and_save_documentation_index(
    documentation_chunks: List[Dict[str, Any]], 
    embedding_model_name: str
):
    """
    Builds a LanceDB index from processed documentation chunks and saves it to disk.
    The database is saved at the path defined by DOCS_DB_PATH.

    Args:
        documentation_chunks: A list of dictionaries, where each dictionary represents a 
                              documentation chunk and contains keys like 'text_chunk', 'url', 
                              'source_file', 'header', 'word_length'.
        embedding_model_name: The name of the embedding model to use.
    """
    if os.path.exists(DOCS_DB_PATH):
        print(f'Removing existing documentation embeddings database: {DOCS_DB_PATH}')
        shutil.rmtree(DOCS_DB_PATH)
    
    vdb = VectorDB("lancedb", uri=DOCS_DB_PATH, embedding=embedding_model_name)
    
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

    print(f'Ingesting {len(texts_for_lancedb)} text chunks into LanceDB at: {DOCS_DB_PATH}')
    vdb.ingest(
        texts=texts_for_lancedb,
        metadatas=all_metadatas_for_lancedb,
    )
    print(f'Documentation embeddings successfully saved to: {DOCS_DB_PATH}')

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