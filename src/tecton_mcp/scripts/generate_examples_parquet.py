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
import re
import json
import pandas as pd
import tqdm
from typing import List, Dict, Any, Tuple
from pydantic import BaseModel, Field
from openai import OpenAI
from pathlib import Path

# Initialize OpenAI client
client = OpenAI()

class Declarations(BaseModel):
    """Model for extracted Tecton declarations."""
    declarations: List[Tuple[str, str]] = Field(
        ..., 
        description="""List of tuples of declarations.
Each tuple contains the object/function name and the description.

You should only extract:
- Tecton classes and decorated functions
- Tecton objects embedded in other objects (e.g. SnowflakeConfig in BatchSource, Attribute and Aggregate)
- Unit tests (set the first value in the tuple as "test")
                                              
Pay attention to the import statements at the beginning that tells you which objects and functions are imported from Tecton.

Don't extract declarations that are commented out

The description should be under 150 words


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
    )


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
            model="gpt-4o-2024-11-20",
            messages=[
                {
                    "role": "system",
                    "content": """Extract ALL Tecton-related declarations from the code with granular detail.

You should extract:
- Tecton classes and decorated functions (Entity, FeatureView, FeatureService, etc.)
- Tecton objects embedded in other objects (SnowflakeConfig, Attribute, Aggregate, etc.)
- Unit tests (mark as "test")

IMPORTANT: Extract individual components separately. For example:
- If there are multiple Aggregate objects in a feature view, extract each one separately
- If there are multiple Attribute objects, extract each one separately
- Extract both the container (e.g., FeatureView) AND its components (e.g., each Aggregate)

For descriptions:
- Keep them concise but informative
- Focus on what the specific component does
- Include key details like time windows, functions, purposes
- Keep descriptions short and focused (under 150 words, typically much shorter)
- Be specific about what each component does rather than generic explanations

Pay attention to import statements to identify Tecton objects.
Don't extract declarations that are commented out with # comments.

Be thorough and granular - extract every meaningful Tecton component you can find."""
                },
                {
                    "role": "user",
                    "content": f"Extract ALL Tecton declarations from this code, being granular with individual components:\n\n{code}"
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
        return result.get("declarations", [])
    
    except Exception as e:
        print(f"Error extracting declarations: {e}")
        return []


def extract_declarations(folders: List[str]) -> List[Dict[str, Any]]:
    """Extract declarations from all Python files in the given folders."""
    files = []
    for folder in folders:
        if os.path.exists(folder):
            files.extend(get_py_files(folder))
        else:
            print(f"Warning: Directory {folder} does not exist, skipping...")
    
    print(f"Found {len(files)} Python files to process")
    
    res = []
    for i in tqdm.tqdm(range(len(files)), desc="Processing files"):
        file_path = files[i]
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
                        "file_path": file_path
                    })
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue
    
    return res


def is_rift_code(file_path: str, code: str) -> bool:
    """Determine if the code is Rift-based or Spark-based based on file path."""
    # Check if file path contains 'rift' directory
    if '/rift/' in file_path.lower() or file_path.lower().endswith('/rift'):
        return True
    
    return False


def main():
    """Main function to generate examples parquet files."""
    # Define the directories to process
    directories = [
        os.path.expanduser("~/git/tecton-sample-repo/spark"),
        os.path.expanduser("~/git/tecton-sample-repo/rift"), 
        os.path.expanduser("~/git/examples/Spark")  # Updated to match actual directory name
    ]
    
    print("Starting extraction of Tecton declarations...")
    print(f"Processing directories: {directories}")
    
    # Extract all declarations
    all_declarations = extract_declarations(directories)
    
    if not all_declarations:
        print("No declarations found. Exiting.")
        return
    
    print(f"Extracted {len(all_declarations)} declarations")
    
    # Separate into Rift and Spark examples
    rift_examples = []
    spark_examples = []
    
    for declaration in all_declarations:
        if is_rift_code(declaration.get("file_path", ""), declaration.get("code", "")):
            rift_examples.append({
                "text": declaration["text"],
                "code": declaration["code"]
            })
        else:
            spark_examples.append({
                "text": declaration["text"], 
                "code": declaration["code"]
            })
    
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