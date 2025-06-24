import json, os, shutil, re
from tecton_mcp.tecton_utils import APIGraphBuilder
from tecton_mcp.tools.example_code_snippet_tools import build_and_save_example_code_snippet_index
from tecton_mcp.tools.documentation_tools import build_and_save_documentation_index as build_docs_lancedb
from tecton_mcp.embed.meta import write_metadata, DATA_DIR
from tecton_mcp.constants import FILE_DIR
from tecton_mcp.embed import VectorDB
import tecton
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration for each documentation version we want to process. The output
# database filename MUST match the resolution logic in
# `documentation_tools._resolve_docs_db_path()`.
# ---------------------------------------------------------------------------


EMBED_MODEL = "sentence-transformers/multi-qa-MiniLM-L6-cos-v1"

DOC_VERSIONS = [
    {
        "name": "latest",  # corresponds to >1.1.x (default / beta)
        "db_filename": "tecton_docs.db",
        "docs_path": os.path.expanduser("~/git/tecton-docs/docs"),
        "base_url": "https://docs.tecton.ai/docs/beta/",
        "chunks_filename": "documentation_chunks.parquet",
    },
    {
        "name": "1.1",
        "db_filename": "tecton_docs_1.1.db",
        "docs_path": os.path.expanduser("~/git/tecton-docs/versioned_docs/version-1.1"),
        "base_url": "https://docs.tecton.ai/docs/",
        "chunks_filename": "documentation_1_1_chunks.parquet",
    },
    {
        "name": "1.0",
        "db_filename": "tecton_docs_1.0.db",
        "docs_path": os.path.expanduser("~/git/tecton-docs/versioned_docs/version-1.0"),
        "base_url": "https://docs.tecton.ai/docs/1.0/",
        "chunks_filename": "documentation_1_0_chunks.parquet",
    },
]


def _sanitize_internal_links(content: str) -> str:
    """
    Sanitize internal markdown links to prevent LLMs from constructing URLs from them.
    
    This removes .md file references that could confuse LLMs into creating malformed URLs
    instead of using the clean Source URLs provided in the metadata.
    
    Args:
        content: The text content that may contain internal .md links
        
    Returns:
        Sanitized content with internal .md links removed or replaced
    """
    # Replace [text](path.md) and [text](path.md#anchor) with just the text (no brackets)
    content = re.sub(r'\[([^\]]+)\]\([^)]*\.md[^)]*\)', r'\1', content)
    
    # Handle .md references in quotes, backticks, and other contexts
    content = re.sub(r'[\'"`]([^\'"`]*\.md[^\'"`]*)[\'"`]', r'the related documentation', content)
    content = re.sub(r'`([^`]*\.md[^`]*)`', r'the related documentation', content)
    
    # Replace standalone .md file references with generic text
    # Handle common patterns like "file.md," "file.md." "file.md " etc.
    content = re.sub(r'(\S+\.md)([.,;:!?\s])', r'the related documentation\2', content)
    content = re.sub(r'(\S+\.md)$', r'the related documentation', content)
    
    # Handle .mdx references too
    content = re.sub(r'(\S+\.mdx)([.,;:!?\s])', r'the related documentation\2', content)
    content = re.sub(r'(\S+\.mdx)$', r'the related documentation', content)
    
    # Clean up any remaining empty brackets that might be left over
    content = re.sub(r'\[\s*\]', '', content)
    
    # Clean up double spaces and punctuation issues
    content = re.sub(r'\s+', ' ', content)  # Multiple spaces to single space
    
    # Note: We avoid aggressive punctuation cleanup to preserve code examples
    # Patterns like ", ." or ".." in code blocks are valid syntax
    
    return content.strip()


def generate_doc_url(file_path: str, docs_root_path: str, base_url: str) -> str:
    """Generates the website URL for a given documentation file path.
    
    Handles deduplication when filename matches the parent directory name.
    For example:
    - 'testing-features/testing-features.md' -> 'testing-features/'
    - 'stream-feature-view/stream-feature-view.md' -> 'stream-feature-view/'
    """
    relative_path = os.path.relpath(file_path, docs_root_path)
    if relative_path.endswith(".md"):
        relative_path = relative_path[:-3]
    
    # Handle duplication: if filename matches the parent directory name, remove the filename
    path_parts = relative_path.split('/')
    if len(path_parts) >= 2 and path_parts[-1] == path_parts[-2]:
        # Remove the duplicated filename, keep the directory path
        relative_path = '/'.join(path_parts[:-1])
    
    # Ensure no leading slash for joining with base_url
    return base_url + relative_path.lstrip('/')


def process_documentation_files(docs_dir: str, base_url: str, output_parquet_path: str):
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
                            # Sanitize internal .md links before saving to parquet
                            sanitized_text = _sanitize_internal_links(text_to_embed)
                            word_length = len(sanitized_text.split())
                            if word_length >= 10:
                                processed_chunks_for_file.append({
                                    "text_chunk": sanitized_text,
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

    # -------------------------------------------------------------------
    # Persist the extracted chunks for debugging / downstream inspection.
    # The caller specifies the full path (including filename) where the
    # Parquet file should be written so that different documentation
    # versions do not clobber one another.
    # -------------------------------------------------------------------

    output_parquet_dir = os.path.dirname(output_parquet_path)
    os.makedirs(output_parquet_dir, exist_ok=True)
    
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
    
    # -------------------------------------------------------------------
    # Generate documentation embeddings for every configured version.
    # -------------------------------------------------------------------

    for cfg in DOC_VERSIONS:
        docs_path = cfg["docs_path"]
        base_url = cfg["base_url"]
        target_db_path = os.path.join(FILE_DIR, "data", cfg["db_filename"])

        print("\n==============================")
        print(f"Processing docs version: {cfg['name']}")
        print(f"Docs path   : {docs_path}")
        print(f"Base URL    : {base_url}")
        print(f"Output DB   : {target_db_path}")
        print("==============================\n")

        # Use the explicitly configured Parquet filename for this docs version.
        chunks_file_path = os.path.join(FILE_DIR, "data", cfg["chunks_filename"])

        # Extract + chunk markdown content
        doc_chunks = process_documentation_files(docs_path, base_url, chunks_file_path)

        if doc_chunks:
            build_docs_lancedb(doc_chunks, EMBED_MODEL, db_path=target_db_path)
        else:
            print(f"Warning: No documentation chunks generated for docs path {docs_path}. Skipping DB creation.")

    write_metadata(EMBED_MODEL, tecton.__version__)
    print(f"Embeddings regenerated and metadata saved at {DATA_DIR}")


if __name__ == "__main__":
    main() 