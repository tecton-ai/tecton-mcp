import json, os, shutil
from tecton_mcp.tecton_utils import APIGraphBuilder
from tecton_mcp.tools.example_code_snippet_tools import build_and_save_example_code_snippet_index
from tecton_mcp.tools.documentation_tools import build_and_save_documentation_index as build_docs_lancedb
from tecton_mcp.embed.meta import write_metadata, DATA_DIR
from tecton_mcp.constants import FILE_DIR
from tecton_mcp.embed import VectorDB
import tecton
import pandas as pd

EMBED_MODEL = "sentence-transformers/multi-qa-MiniLM-L6-cos-v1"
DOCS_PATH = os.path.expanduser("~/git/tecton-docs/docs")
DOCS_BASE_URL = "https://docs.tecton.ai/docs/beta/"


def generate_doc_url(file_path: str, docs_root_path: str, base_url: str) -> str:
    """Generates the website URL for a given documentation file path."""
    relative_path = os.path.relpath(file_path, docs_root_path)
    if relative_path.endswith(".md"):
        relative_path = relative_path[:-3]
    # Ensure no leading slash for joining with base_url
    return base_url + relative_path.lstrip('/')


def process_documentation_files(docs_dir: str, base_url: str):
    """Walks through docs_dir, chunks .md files, saves chunks to Parquet, and returns chunk data."""
    print(f"Starting documentation file processing from: {docs_dir}")
    
    all_chunk_data_for_parquet = [] 
    
    for root, _, files in os.walk(docs_dir):
        for file in files:
            if file.endswith(".md"):
                file_path = os.path.join(root, file)
                print(f"Processing documentation file: {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    lines = content.splitlines()
                    doc_url = generate_doc_url(file_path, docs_dir, base_url)
                    
                    current_section_lines = []
                    current_section_header_text = None 
                    processed_chunks_for_file = []
                    in_code_block = False 

                    def process_section(header_text, section_lines, file_url, source_path):
                        text_to_embed = ""
                        if header_text: 
                            text_to_embed += header_text + "\n"
                        text_to_embed += "\n".join(section_lines) 
                        text_to_embed = text_to_embed.strip()

                        if text_to_embed: 
                            word_length = len(text_to_embed.split())
                            if word_length >= 10:
                                processed_chunks_for_file.append({
                                    "text_chunk": text_to_embed,
                                    "header": header_text.strip() if header_text else "N/A", 
                                    "url": file_url,
                                    "source_file": source_path,
                                    "word_length": word_length
                                })

                    for line_raw in lines:
                        stripped_line = line_raw.lstrip()

                        if stripped_line.startswith("```"):
                            in_code_block = not in_code_block
                            current_section_lines.append(line_raw)
                            continue 

                        is_top_level_header_line = False
                        if not in_code_block and stripped_line.startswith('#'):
                            if not stripped_line.startswith('##'): 
                                temp_after_hash = stripped_line.lstrip('#')
                                if not temp_after_hash or temp_after_hash.startswith(' ') or all(c == '#' for c in stripped_line):
                                    is_top_level_header_line = True

                        if is_top_level_header_line:
                            if current_section_header_text or current_section_lines:
                                process_section(current_section_header_text, current_section_lines, doc_url, file_path)
                            
                            current_section_header_text = line_raw 
                            current_section_lines = [] 
                        else:
                            current_section_lines.append(line_raw)

                    process_section(current_section_header_text, current_section_lines, doc_url, file_path)
                    
                    all_chunk_data_for_parquet.extend(processed_chunks_for_file)
                                
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

    if not all_chunk_data_for_parquet:
        print("No markdown files found or no content extracted. Skipping Parquet file creation.")
        return [] # Return empty list if no data

    output_parquet_dir = os.path.join(FILE_DIR, "data")
    os.makedirs(output_parquet_dir, exist_ok=True)
    output_parquet_path = os.path.join(output_parquet_dir, "documentation_chunks.parquet")
    
    df_chunks = pd.DataFrame(all_chunk_data_for_parquet)
    try:
        df_chunks.to_parquet(output_parquet_path, index=False)
        print(f"Documentation chunks saved to Parquet file: {output_parquet_path}")
    except Exception as e:
        print(f"Error saving Parquet file to {output_parquet_path}: {e}")
    
    return all_chunk_data_for_parquet # Return the processed chunk data


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    examples = pd.read_parquet(os.path.join(FILE_DIR, "data", "examples.parquet")).to_dict(orient="records")
    build_and_save_example_code_snippet_index(examples, EMBED_MODEL)
    
    # Get processed documentation chunks
    documentation_chunks_data = process_documentation_files(DOCS_PATH, DOCS_BASE_URL)
    
    # Build LanceDB index using the new tool if data exists
    if documentation_chunks_data:
        build_docs_lancedb(documentation_chunks_data, EMBED_MODEL) 
    else:
        # Raise an exception if no documentation chunks were processed
        raise ValueError("No documentation chunks were processed from DOCS_PATH. Halting execution. Check path and chunking criteria.")

    write_metadata(EMBED_MODEL, tecton.__version__)
    print(f"Embeddings regenerated and metadata saved at {DATA_DIR}")


if __name__ == "__main__":
    main() 