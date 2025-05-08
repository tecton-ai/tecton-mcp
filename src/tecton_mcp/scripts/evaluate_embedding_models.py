import os
import json
import statistics
from typing import List, Dict, Callable
import shutil

import openai
from tecton_mcp.tecton_utils import APIGraphBuilder
from tecton_mcp.tools.api_reference_tools import build_and_save_api_reference_index
from tecton_mcp.tools.example_code_snippet_tools import (
    build_and_save_example_code_snippet_index,
)
from tecton_mcp.embed.vector_db import VectorDB, _make_embedding_model
from tecton_mcp.constants import FILE_DIR
import pandas as pd
import lancedb

openai.api_key = os.getenv("OPENAI_API_KEY")
JUDGE_MODEL = "gpt-4o-mini"

# --------------------------- Helpers ---------------------------------

def ask_llm(prompt: str) -> str:
    resp = openai.chat.completions.create(
        model=JUDGE_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()


# --------------------------- Test‑case generation ---------------------

def generate_test_cases(n: int = 10) -> List[str]:
    """Ask the judge LLM to come up with diverse retrieval queries for Tecton examples."""
    try:
        examples = pd.read_parquet(os.path.join(FILE_DIR, "data", "examples.parquet")).to_dict(
            orient="records"
        )
        context_str = "\n".join([f"- {x['text']}" for x in examples])
    except Exception as e:
        print(f"Warning: Could not load examples for query generation context: {e}")
        context_str = "- Various Tecton feature definitions and tests."

    prompt = f"""
You are helping evaluate Tecton code example retrieval.
Generate {n} diverse, realistic natural language queries that a Tecton user might ask to find specific examples of **feature engineering** techniques in Tecton.

Consider the types of examples likely available (you don't have the full list, but here are a few snippets to give you an idea):
{context_str}

Focus on queries about feature transformations, aggregations, time-windowing, handling different data types, SQL usage for features, etc. Generate queries similar in style and topic to these examples, but ensure diversity:
- Spark Batch Feature View example
- SnowflakeSQL aggregation example
- Stream Feature example with an approx percentile aggregation
- Realtime Feature that compares 2 different features
- Batch feature that has a complex SQL group by statement and joins multiple tables together

Return the queries ONLY as a JSON list of strings (e.g., ["query 1", "query 2", ...]) with absolutely no additional text, commentary, or markdown formatting.
"""
    txt = ask_llm(prompt)
    try:
        return json.loads(txt)
    except Exception:
        # fallback: split lines
        return [line.strip("- ") for line in txt.splitlines() if line.strip()]


# --------------------------- Build retrievers per model --------------

def build_retriever_for_examples(embedding_model: str):
    examples = pd.read_parquet(os.path.join(FILE_DIR, "data", "examples.parquet")).to_dict(
        orient="records"
    )
    # Use distinct paths per model to avoid schema conflicts
    safe_model_name = embedding_model.replace('/', '_').replace('-', '_')
    vpath = f"/tmp/eval_examples_{safe_model_name}.db"

    # Mimic working code: remove entire directory before init
    shutil.rmtree(vpath, ignore_errors=True)

    vdb = VectorDB("lancedb", uri=vpath, embedding=embedding_model)

    vdb.ingest(
        texts=[x["text"] for x in examples],
        # Mimic working code: include title in metadata
        metadatas=[dict(code=x["code"], title=x["text"]) for x in examples],
    )

    @vdb.retriever(name="examples", top_k=10)
    def _vector_retriever(query: str, filter=None, result=None) -> str:
        # The VectorDB retriever decorator injects the result
        # We need to format it for comparison
        if result is None: return "" # Should not happen with decorator
        return "\n\n---\n\n".join(item["code"] for item in result)

    return _vector_retriever


# --------------------------- Evaluation ------------------------------

def judge_query(query: str, outputs: Dict[str, str]) -> Dict[str, int]:
    prompt = (
        "You are judging the relevance of retrieval outputs for the query:\n"
        f"{query}\n\n"
        "Each candidate is labeled by MODEL_NAME. Score each on a 1‑10 scale.\n"
        "Return JSON mapping model names to scores.\n\n"
    )
    for name, out in outputs.items():
        prompt += f"MODEL {name}:\n{out}\n\n"

    txt = ask_llm(prompt)
    # print(f"\n--- LLM Judge Raw Response for Query: ---\n{query}\n---\n{txt}\n---")

    # Clean markdown fences potentially added by LLM
    txt = txt.strip()
    if txt.startswith("```json"):
        txt = txt[7:]
    if txt.endswith("```"):
        txt = txt[:-3]
    txt = txt.strip()

    # print(f"DEBUG: Cleaned text before JSON attempt:\n>>>\n{txt}\n<<<\n") # Commented out debug print

    try:
        return {k: int(v) for k, v in json.loads(txt).items()}
    except Exception as e:
        print(f"ERROR: JSON parsing failed: {e}")
        print("Attempting fallback parsing...")
        # naive parse fallback
        res = {}
        for line in txt.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                k_cleaned = k.strip().strip('" ')
                v_cleaned = v.strip().rstrip(',').strip()
                try:
                    res[k_cleaned] = int(v_cleaned)
                except ValueError:
                    print(f"Fallback failed to parse value for key '{k_cleaned}': {v_cleaned}")
                    pass
        if not res:
            print("Fallback parsing also failed to extract any scores.")
        else:
            print(f"Fallback parsing successfully extracted: {res}")
        return res


# --------------------------- Main routine ----------------------------

def evaluate_retrievers(retriever_names: List[str], num_cases: int = 10):
    queries = generate_test_cases(num_cases)
    print("--- Test Queries ---")
    for i, q in enumerate(queries):
        print(f"{i+1}. {q}")
    print("--------------------\n")

    # build retrievers per model
    retrievers: Dict[str, Callable[[str], str]] = {}
    for name in retriever_names:
        if name == "jina":
            retrievers["jina"] = build_retriever_for_examples("jinaai/jina-embeddings-v2-base-code")
        elif name == "openai_small":
            retrievers["openai_small"] = build_retriever_for_examples("openai/text-embedding-3-small")
        elif name == "minilm_l6":
            retrievers["minilm_l6"] = build_retriever_for_examples("sentence-transformers/all-MiniLM-L6-v2")
        elif name == "minilm_l12":
            retrievers["minilm_l12"] = build_retriever_for_examples("sentence-transformers/all-MiniLM-L12-v2")
        elif name == "bge_small":
            retrievers["bge_small"] = build_retriever_for_examples("BAAI/bge-small-en")
        elif name == "gte_small":
            retrievers["gte_small"] = build_retriever_for_examples("thenlper/gte-small")
        elif name == "multi_qa_minilm":
            retrievers["multi_qa_minilm"] = build_retriever_for_examples("sentence-transformers/multi-qa-MiniLM-L6-cos-v1")
        else:
            # Assume it's an embedding model name if not 'bm25'
            retrievers[name] = build_retriever_for_examples(name)

    scores: Dict[str, List[int]] = {name: [] for name in retriever_names}
    all_query_results = [] # Store results for final summary

    for q in queries:
        print(f"\n--- Evaluating Query: {q} ---")
        outputs = {name: retriever_func(query=q) for name, retriever_func in retrievers.items()}
        all_query_results.append({"query": q, "outputs": outputs})
        judged = judge_query(q, outputs)
        for m, sc in judged.items():
            if m in scores: # Judge might return models not in our list
                scores[m].append(sc)

    # print stats
    for m, lst in scores.items():
        if lst:
            mean = statistics.mean(lst)
            med = statistics.median(lst)
            print(f"{m}: mean={mean:.2f}, median={med:.1f}, n={len(lst)}")
        else:
            print(f"{m}: no scores")

    # Generate final summary analysis
    print("\n--- Generating Final Summary Analysis --- ")
    summary_prompt = "You have evaluated several retrievers based on their output for multiple queries.\n"
    summary_prompt += "Here is the full data:\n\n"

    for result in all_query_results:
        summary_prompt += f"Query: {result['query']}\n"
        for name, out in result['outputs'].items():
            # Limit output length in summary prompt
            out_snippet = out[:500] + ("..." if len(out) > 500 else "")
            summary_prompt += f"  MODEL {name}:\n{out_snippet}\n"
        summary_prompt += "---\n"

    summary_prompt += "\nBased on all the queries and retriever outputs above, please provide a brief qualitative summary analysis.\n"
    # Dynamically list all tested retrievers
    model_list_str = ", ".join(retriever_names)
    summary_prompt += f"Compare the overall performance, strengths, and weaknesses of the different retrievers ({model_list_str})."

    summary_analysis = ask_llm(summary_prompt)
    print("\n--- LLM Summary Analysis ---")
    print(summary_analysis)


if __name__ == "__main__":
    # Simplify to test only OpenAI
    RETRIEVERS_TO_TEST = [
        "jina", # Corresponds to jinaai/jina-embeddings-v2-base-code
        "openai_small", # Corresponds to openai/text-embedding-3-small
        "minilm_l6", # Added sentence-transformers/all-MiniLM-L6-v2
        "minilm_l12",# Added sentence-transformers/all-MiniLM-L12-v2
        "bge_small",       # Added BAAI/bge-small-en
        "gte_small",       # Added thenlper/gte-small
        "multi_qa_minilm", # Added sentence-transformers/multi-qa-MiniLM-L6-cos-v1
    ]

    evaluate_retrievers(RETRIEVERS_TO_TEST, num_cases=50)

    # evaluate_retrievers(RETRIEVERS_TO_TEST, num_cases=2) # Reduced for debugging score parsing 