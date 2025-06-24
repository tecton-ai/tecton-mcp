#!/usr/bin/env python3
"""
Generate examples parquet files from Tecton code repositories.

This script replaces the outdated generate_examples_parquet.ipynb notebook
that relied on the deprecated tecton_gen_ai library. It uses OpenAI's API
directly to extract Tecton declarations from Python files.

The script processes code from specified directories and generates two
separate parquet files:
- examples_spark.parquet: For Spark-based Tecton code
- examples_rift.parquet: For Rift-based Tecton code
"""

import os
import json
from typing import List, Dict, Any, Tuple

import pandas as pd
import tqdm
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI()

# Import shared repository utilities
from tecton_mcp.utils.repo_utils import DirectoryConfig, DirectoryType


def get_py_files(directory: str) -> List[str]:
    """Recursively find all Python files in a directory."""
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.py'):
                files.append(os.path.join(root, filename))
    return files


def extract_declarations_from_code(code: str) -> List[Tuple[str, str]]:
    """Extract Tecton declarations from Python code using OpenAI."""
    try:
        response = client.chat.completions.create(
            model="gpt-4.1",  
            messages=[
                {
                    "role": "system",
                    "content": """Extract Tecton-related declarations from the code, as a list of tuples of declarations.
Each tuple contains the object/function name and the description.

YOU SHOULD EXTRACT:
- Tecton classes and decorated functions (Entity, FeatureView, FeatureService, etc.)
- Tecton objects embedded in other objects (SnowflakeConfig, Attribute, Aggregate, etc.)
- Unit tests (describe explcitly as "Unit test" and then describe what it's testing)

Pay attention to import statements to identify Tecton objects.
Don't extract declarations that are commented out with # comments.

CRITICAL REQUIREMENTS FOR DESCRIPTIONS:
1. ALWAYS use comments above the declaration 
2. ALWAYS use the actual `description` field from the code (e.g., FeatureView.description, Entity.description)
3. If neither description field nor comments exist, infer the business purpose from context and variable names
4. Focus on WHAT the component does for the business, in addition to technical details

EXAMPLES OF GOOD vs BAD descriptions:

GOOD:
- "Article interactions: aggregations of clicks, carts, orders on an article" (uses actual description field)
- "Unique sessions with article interactions over the past 30 days" (business meaning)
- "Distance in kilometers between transaction and user's home location, used for fraud detection"
- "Transaction request source schema" (from comments)

BAD:
- "article_sessions" (just the function name)
- "transactions_batch" (just the variable name)
- "ad_impressions_batch" (just the variable name)

For each declaration type, use the description parameter value AND comments if they exist, then also infer purpose from context

Focus on extracting components that would be useful for someone learning Tecton or implementing similar features.

MORE EXAMPLES:

For example, with this code:

```from tecton import Entity, FeatureTable, Attribute
from tecton.types import String, Timestamp, Int64, Field
from fraud.entities import user
from datetime import timedelta


features = [
    Attribute('user_login_count_7d', Int64),
    Attribute('user_login_count_30d', Int64),
]

user_login_counts = FeatureTable(
    name='user_login_counts',
    entities=[user],
    features=features,
    online=True,
    offline=True,
    ttl=timedelta(days=7),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description='User login counts over time.',
    timestamp_field='timestamp'
)
```

The declarations would be:

[("FeatureTable", "User login counts over time.")]

In this code

```python
fraud_detection_feature_service = FeatureService(
    name='fraud_detection_feature_service',
    prevent_destroy=False,  # Set to True for production services to prevent accidental destructive changes or downtime.
    features=[
        transaction_amount_is_higher_than_average,
        user_transaction_amount_metrics,
        user_transaction_counts,
        user_distinct_merchant_transaction_count_30d,
        merchant_fraud_rate
    ]
)

minimal_fs = FeatureService(
     name='minimal_fs',
     features=[
         transaction_amount_is_high
     ]
)
```

The declarations would be:

[
    ("FeatureService", "Fraud detection feature service"),
    ("FeatureService", "Whether transaction amount is higher")
]

In this code:

```
import math

from ads.features.on_demand_feature_views.user_query_embedding_similarity import user_query_embedding_similarity


# Testing the 'user_query_embedding_similarity' feature which takes in request data ('query_embedding')
# and a precomputed feature ('user_embedding') as inputs
def test_user_query_embedding_similarity():
    request = {'query_embedding': [1.0, 1.0, 0.0]}
    user_embedding = {'user_embedding': [0.0, 1.0, 1.0]}

    actual = user_query_embedding_similarity.test_run(request=request, user_embedding=user_embedding)

    # Float comparison.
    expected = 0.5
    assert math.isclose(actual['cosine_similarity'], expected)
```

The declarations would be:

[("test", "Testing the 'user_query_embedding_similarity' feature which takes in request data ('query_embedding') and a precomputed feature ('user_embedding') as inputs")]

In this code
                                              
```python
from tecton import BatchSource, SnowflakeConfig
from tecton.types import Field, Int64, String, Timestamp, Array

gaming_user_batch = BatchSource(
    name="gaming_users",
    batch_config=SnowflakeConfig(
      database="VINCE_DEMO_DB",
      schema="PUBLIC",
      table="ONLINE_GAMING_USERS",
      url="https://<your-cluster>.<your-snowflake-region>.snowflakecomputing.com/",
      warehouse="COMPUTE_WH",
      timestamp_field='TIMESTAMP',
    ),
)
```

(Pay attention that SnowflakeConfig is a configuration object embedded in the BatchSource object, we also need to extract that)

The declarations would be:

[("BatchSource", "Gaming users batch source"), ("SnowflakeConfig", "Gaming users batch source configuration")]  

In this code:
```
# The following defines several sliding time window aggregations over a user's transaction amounts
@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode='pandas',
    batch_schedule=timedelta(days=1), # Defines how frequently batch jobs are scheduled to ingest into the offline store
    features=[
        Aggregate(input_column=Field('amt', Float64), function='sum', time_window=timedelta(hours=1)),
        Aggregate(input_column=Field('amt', Float64), function='max', time_window=timedelta(days=1)),
        Aggregate(input_column=Field('amt', Float64), function='min', time_window=timedelta(days=3)),
        Aggregate(input_column=Field('amt', Float64), function=approx_percentile(percentile=0.5, precision=100), time_window=timedelta(hours=1))
    ],
    timestamp_field='timestamp',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Transaction amount statistics and total over a series of time windows, updated every 10 minutes.',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
)
def user_transaction_amount_metrics(transactions):
    return transactions[['user_id', 'amt', 'timestamp']]
```
                                              
[("Aggregate", "sum of transaction amounts over the past hour"), ("Aggregate", "max of transaction amounts over the past day"), ("Aggregate", "min of transaction amounts over the past 3 days"), ("Aggregate", "50th percentile of transaction amounts over the past hour")]
"""
                },
                {
                    "role": "user",
                    "content": f"Extract meaningful Tecton declarations from this code. Use comments and description fields when available:\n\n{code}"
                }
            ],
            temperature=0,
            max_tokens=4000,
            timeout=45,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "declarations",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "declarations": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "minItems": 2,
                                    "maxItems": 2
                                }
                            }
                        },
                        "required": ["declarations"],
                        "additionalProperties": False
                    }
                }
            }
        )
        
        result = json.loads(response.choices[0].message.content)
        declarations = result.get("declarations", [])
        
        # Return all declarations without validation filtering
        return declarations
    
    except Exception as e:
        print(f"Error extracting declarations: {e}")
        return []


def extract_declarations(directory_configs: List[DirectoryConfig]) -> List[Dict[str, Any]]:
    """Extract declarations from all Python files in the given directory configurations."""
    files_with_types = []
    
    for config in directory_configs:
        resolved_path = config.resolve_path()
        if resolved_path and os.path.exists(resolved_path):
            folder_files = get_py_files(resolved_path)
            for file_path in folder_files:
                files_with_types.append((file_path, config.directory_type))
        else:
            print(f"Warning: Directory {config.remote_url}/{config.sub_dir} could not be resolved, skipping…")
    
    print(f"Found {len(files_with_types)} Python files to process")
    
    res = []
    for i in tqdm.tqdm(range(len(files_with_types)), desc="Processing files"):
        file_path, directory_type = files_with_types[i]
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # Skip empty files or files with very little content
            if len(code.strip()) < 50:
                continue
                
            declarations = extract_declarations_from_code(code)
            for declaration in declarations:
                if len(declaration) >= 2:
                    res.append({
                        "text": f"Example of {declaration[0]}. {declaration[1]}", 
                        "code": code,
                        "file_path": file_path,
                        "directory_type": directory_type
                    })
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue
    
    return res


def main():
    """Main function to generate examples parquet files."""
    # Define the directories to process with their types
    directory_configs = [
        # Directories from tecton-sample-repo
        DirectoryConfig(
            DirectoryType.RIFT,
            remote_url="https://github.com/tecton-ai/tecton-sample-repo.git",
            sub_dir="rift",
        ),
        DirectoryConfig(
            DirectoryType.SPARK,
            remote_url="https://github.com/tecton-ai/tecton-sample-repo.git",
            sub_dir="spark",
        ),

        # Directories from tecton-ai/examples
        DirectoryConfig(
            DirectoryType.RIFT,
            remote_url="https://github.com/tecton-ai/examples.git",
            sub_dir="Snowflake",
        ),
        DirectoryConfig(
            DirectoryType.SPARK,
            remote_url="https://github.com/tecton-ai/examples.git",
            sub_dir="Spark",
        ),
    ]
    
    print("Starting extraction of Tecton declarations...")
    dir_labels = [f"{cfg.remote_url}/{cfg.sub_dir}" for cfg in directory_configs]
    print(f"Processing directories: {dir_labels}")
    
    # Extract all declarations with type information
    all_declarations = extract_declarations(directory_configs)
    
    if not all_declarations:
        print("No declarations found. Exiting.")
        return
    
    print(f"Extracted {len(all_declarations)} declarations")
    
    # Separate into Rift and Spark examples based on directory_type
    rift_examples = []
    spark_examples = []
    
    for declaration in all_declarations:
        example_data = {
            "text": declaration["text"],
            "code": declaration["code"]
        }
        
        if declaration["directory_type"] == DirectoryType.RIFT:
            rift_examples.append(example_data)
        else:
            spark_examples.append(example_data)
    
    print(f"Found {len(rift_examples)} Rift examples and {len(spark_examples)} Spark examples")
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save Rift examples
    if rift_examples:
        rift_df = pd.DataFrame(rift_examples)
        rift_output_path = os.path.join(output_dir, "examples_rift.parquet")
        rift_df.to_parquet(rift_output_path, index=False)
        print(f"Saved {len(rift_examples)} Rift examples to {rift_output_path}")
    else:
        print("No Rift examples found")
    
    # Save Spark examples  
    if spark_examples:
        spark_df = pd.DataFrame(spark_examples)
        spark_output_path = os.path.join(output_dir, "examples_spark.parquet")
        spark_df.to_parquet(spark_output_path, index=False)
        print(f"Saved {len(spark_examples)} Spark examples to {spark_output_path}")
    else:
        print("No Spark examples found")
    
    # Also save combined examples (for backward compatibility)
    combined_examples = rift_examples + spark_examples
    if combined_examples:
        combined_df = pd.DataFrame(combined_examples)
        combined_output_path = os.path.join(output_dir, "examples.parquet")
        combined_df.to_parquet(combined_output_path, index=False)
        print(f"Saved {len(combined_examples)} combined examples to {combined_output_path}")
    
    print("Example extraction completed successfully!")


if __name__ == "__main__":
    main() 