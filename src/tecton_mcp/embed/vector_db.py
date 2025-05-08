"""
Simplified VectorDB implementation that provides only the essential functionality.
"""

import json
import inspect
import shutil
from typing import Any, Callable, Dict, List, Optional, Type, Union
from functools import lru_cache

from langchain_core.vectorstores import VectorStore
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_community.vectorstores.lancedb import LanceDB
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from pydantic import BaseModel, Field, create_model
from tecton_mcp.embed.meta import get_embedding_model

_DEFAULT_SEARCH_TYPE = "mmr"
_DEFAULT_TOP_K = 5


@lru_cache(maxsize=None)
def _make_embedding_model(embedding_name: str | None = None) -> Embeddings:
    """Create (or fetch cached) embedding model; always trust remote code for HF models."""
    if embedding_name is None:
        embedding_name = get_embedding_model()

    if embedding_name.startswith("openai/"):
        model_id = embedding_name.split("/", 1)[1]
        return OpenAIEmbeddings(model=model_id)
    else:
        return HuggingFaceEmbeddings(
            model_name=embedding_name,
            model_kwargs={"trust_remote_code": True},
        )


def _make_vector_db(provider: str, embedding: Embeddings, **kwargs) -> VectorStore:
    """Create a vector database based on the provided provider."""
    if provider == "lancedb":
        return LanceDB(embedding=embedding, **kwargs)
    else:
        raise ValueError(f"Unsupported vector db provider: {provider}")


class VectorDB:
    """Simplified VectorDB implementation for embedding and retrieval."""
    
    def __init__(self, provider: str, *, embedding: str, **kwargs):
        """Initialize a VectorDB instance.

        Args:
            provider: Vector store provider (e.g., "lancedb")
            embedding: Embedding model identifier
            **kwargs: Extra args passed to the vector store constructor (e.g., uri)
        """

        self.provider = provider
        self.embedding_name = embedding

        # Always trust remote code when loading HF models
        embedding_obj = _make_embedding_model(embedding_name=embedding)

        # Create vector store
        self.instance = _make_vector_db(provider=provider, embedding=embedding_obj, **kwargs)
    
    def ingest(
        self,
        texts: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
    ):
        """
        Ingest texts and metadata into the vector database.
        
        Args:
            texts: The texts to ingest
            metadatas: Optional metadata for each text
            ids: Optional IDs for each text
        """
        if ids is None:
            import hashlib
            ids = [hashlib.md5(text.encode()).hexdigest() for text in texts]
        if metadatas is None:
            metadatas = [{} for _ in texts]
        # TEMP: Ingest without metadata to debug ArrowInvalid error
        self.instance.add_texts(texts=texts, metadatas=metadatas, ids=ids)
    
    def search(
        self,
        query: str,
        top_k: int = _DEFAULT_TOP_K,
        search_type: str = _DEFAULT_SEARCH_TYPE,
        filter: Optional[Dict[str, Any]] = None,
        text_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search the vector database for similar texts.
        
        Args:
            query: The search query
            top_k: The number of results to return
            search_type: The search type (e.g., "mmr", "similarity")
            filter: Optional filter to apply to the search
            text_key: Optional key to store the text in the result metadata
            
        Returns:
            A list of metadata dictionaries from the search results
        """
        params = {}
        
        # Handle filter for LanceDB
        if filter is not None and len(filter) > 0 and isinstance(self.instance, LanceDB):
            expr = " AND ".join([self._kv_to_sql(k, v) for k, v in filter.items()])
            if expr != "":
                params["filter"] = expr
                params["prefilter"] = params.get("prefilter", True)
        elif filter is not None and len(filter) > 0:
            params["filter"] = filter
            
        # Perform search
        docs = self.instance.search(
            query=query,
            k=top_k,
            search_type=search_type,
            **params,
        )
        
        # Process results
        results = []
        for doc in docs:
            if text_key is None:
                results.append(doc.metadata)
            else:
                metadata = doc.metadata.copy()
                metadata[text_key] = doc.page_content
                results.append(metadata)
        
        return results
    
    def _kv_to_sql(self, key: str, value: Any) -> str:
        """Convert a key-value pair to a SQL condition for LanceDB."""
        key = "metadata." + key
        if isinstance(value, str):
            return f"{key} = '{value}'"
        if isinstance(value, bool):
            return f"{key}" if value else f"NOT {key}"
        if isinstance(value, (int, float)):
            return f"{key} = {value}"
        raise ValueError(f"Unsupported value type {type(value)}")
    
    def retriever(
        self,
        func: Any = None,
        *,
        name: Optional[str] = None,
        filter: Optional[Type[BaseModel]] = None,
        top_k: int = _DEFAULT_TOP_K,
        search_type: str = _DEFAULT_SEARCH_TYPE,
        text_key: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Create a retriever function that uses this vector database.
        
        Args:
            func: The retriever function
            name: Optional name for the retriever
            filter: Optional filter model
            top_k: The number of results to return
            search_type: The search type
            text_key: Optional key to store the text in the result metadata
            description: Optional description for the retriever
        """
        def _wrapper(_func: Callable):
            if set(inspect.getfullargspec(_func).args) != set(
                ["query", "filter", "result"]
            ):
                raise ValueError(
                    "Function must have and only have the following input variables: "
                    "`def func(query: str, filter: dict, result: list[dict[str, Any]])`"
                )
            
            # Create retriever function
            def retriever_fn(query: str, **filter_kwargs):
                # Use the vector DB to search
                results = self.search(
                    query=query,
                    top_k=top_k,
                    search_type=search_type,
                    filter=filter_kwargs,
                    text_key=text_key,
                )
                
                # Call the user-provided function
                return _func(query=query, filter=filter_kwargs, result=results)
            
            # Set metadata on the function
            retriever_fn.__name__ = name or _func.__name__
            retriever_fn.__doc__ = description or _func.__doc__
            
            return retriever_fn
            
        if func is None:
            return lambda _func: _wrapper(_func)
        return _wrapper(func) 