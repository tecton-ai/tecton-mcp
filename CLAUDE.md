# Claude Configuration for Tecton Feature Development

This file contains configuration and rules for Claude Code when working with Tecton feature repositories.

## Tecton Feature Development Rules

### General Feature Creation Guidelines

- Break down feature implementation into stages:
  1. Fetch and review relevant rules
  2. Search for Tecton examples and API reference
  3. Look at existing feature repository for reusable Entities and Data Sources. If you find good fits, confirm with the user that they want to reuse.
  4. Implement the solution

- **CRITICAL**: Before writing or editing feature code:
  - You MUST call mcp_tecton_query_example_code_snippet_index_tool to look for relevant code snippets
  - You MUST then think about all the available Tecton classes that you will use to implement the feature
  - You MUST then call mcp_query_tecton_sdk_reference_tool to look at the exact definition of all the Tecton classes and functions you're planning to use
  - You MUST finally come up with a plan that explains which parameters of BatchFeatureView, StreamFeatureView or RealTimeFeatureView you're planning to use based on the SDK reference

### Feature View Selection Rules

- For aggregations over time windows ≤ 60 minutes, default to StreamFeatureView unless specified otherwise
- Never combine `Attribute` and `Aggregate` features in the same FeatureView
- Only supported `mode` values: "pyspark" and "spark_sql" (Rift not supported)

### SQL Handling and Data Source References

- **CRITICAL**: If a user provides a SQL query for a feature: You MUST NOT translate the SQL logic into another language or API (e.g., PySpark DataFrames, Pandas) unless the user explicitly asks you to 'translate to PySpark' or 'rewrite using PySpark DataFrames'. Stick to SQL - it's ok to translate from say Snowflake SQL to Spark SQL if necessary.
- Be very thoughtful when you change a customer provided SQL statement. Make sure you don't remove anything that may be relevant to the feature transformation.

- **CRITICAL**: Every table referenced in SQL (FROM/JOIN) must be declared as a Data Source and included in sources parameter
- When referencing data sources in SQL f-strings, use parameter names from function signature, not hardcoded table names
- Use `unfiltered()` on BatchSource when referencing dimension tables
- For fact tables with timestamps, only use `unfiltered()` if `incremental_backfills=True`

Example:
```python
@batch_feature_view(
    sources=[transactions, products.unfiltered(), stores.unfiltered()],
)
def feature_view_function(transactions, products, stores, context=materialization_context()):
    return f"""
    SELECT * FROM {transactions} t
    JOIN {products} p ON t.product_id = p.product_id
    JOIN {stores} s ON t.store_id = s.store_id
    """
```

### Time and Context Rules

- Never use `CURRENT_DATE()` or similar functions in transformations
- Always use `end_time` from the context parameter
- Never have Python comments (prefixed with "#") in SQL statements

### Incremental Processing Rules

- If BatchFeatureView uses GROUP BY, set `incremental_backfills=True`
- If `incremental_backfills=True`, all sources must use `unfiltered()`
- Handle data filtering manually in the FeatureView function when using incremental backfills

### Aggregation Engine Rules

- Never set `incremental_backfill=True` for FeatureViews using `Aggregate` class
- Built-in Aggregates: approx_count_distinct, approx_percentile, count, first_distinct, first, last_distinct, last, max, mean, min, stddev_pop, stddev_samp, sum, var_pop, var_samp
- BFVs using Aggregate features should set `incremental_materialization=False`

### Snowflake Integration

- Use `BatchSource` with `SnowflakeConfig` for Snowflake data sources

### Entity Creation

- Wrap join keys in `Field` class:
```python
from tecton import Entity
from tecton.types import Field, String

customer = Entity(name="Customer", join_keys=[Field("customer_id", String)])
```

### Validation and Testing

- Validate implementation by running `tecton plan`
- Ensure current directory is a Tecton repository (contains `.tecton` file)
- Never run `tecton apply` or `tecton init` unless explicitly requested
- Create unit tests only after successful `tecton plan` validation

## Tecton Optimization Rules

### Use Tecton Aggregates Over Custom Aggregations

- Check if custom aggregations can be expressed using built-in Aggregate functions
- If **all** can be mapped to Aggregate class, move them out of transformation function
- If **only some** can be expressed, factor those into separate FeatureView
- If aggregation **cannot** be expressed with Aggregate, use `incremental_materialization=True`

### Critical Notes for Optimization

- Review unit testing rules when switching to Aggregation features
- Never change the `mode` of FeatureView when switching to Aggregation
- Look for opportunities to filter data sources when switching to Aggregation
- Study examples of Tecton Aggregation features before implementation

## Unit Testing Rules

### Prerequisites

- Ensure `tecton plan` works before adding unit tests
- Set `conf.set("TECTON_SKIP_OBJECT_VALIDATION", "True")` for mock testing
- Always run tests with `tecton test`, never invoke `pytest` directly

### Test Method Selection

**Use `get_features_for_events` when:**
- Testing features using Tecton's Aggregation Engine
- Testing `@batch_feature_view` with `Aggregate` features

**Use `run_transformation` when:**
- Testing custom SQL transformations without `Aggregate` features
- Verifying raw transformation output
- Testing Realtime Feature Views

### Test Structure Best Practices

1. **Test Data Setup**
   - Clear docstring describing test purpose
   - Minimal but complete mock input data
   - Document purpose of each test record
   - Include edge cases (nulls, zeros, boundaries)

2. **Expected Values**
   - Calculate expected values manually first
   - Document calculations in comments
   - Break down complex calculations into steps
   - Consider edge cases and expected outcomes

3. **Feature Computation**
   - Choose appropriate test method
   - Set up correct time windows
   - Ensure all required mock inputs provided
   - Consider batch schedule constraints

4. **Assertions**
   - Group assertions by feature type
   - Test both column presence and values
   - Include descriptive error messages
   - Use appropriate tolerance for floating-point comparisons
   - Test NULL values explicitly where expected

### Common Testing Pitfalls to Avoid

- Using PySpark calculations for expected values
- Not documenting reason for NULL expectations
- Missing edge cases in test data
- Not accounting for time window boundaries
- Using exact equality for floating-point comparisons
- Not testing both presence and values of features
- Insufficient error messages in assertions
- Not considering batch schedule constraints
- Using deprecated `get_historical_features` method
- Don't just give up and end up skipping a unit test you've created. Fix it! If you don't know how, ask the user for help
- Don't ever try to use the `get_historical_features` method. It's deprecated

### Debugging Unit Tests

If unit tests don't return expected values, try the following techniques:

- If the FeatureView is defined using SQL, print the fully assembled SQL query. Analyze the query to ensure it aligns with your expectations.
  - A common issue is unintended filtering—especially on time ranges—which can silently exclude expected data.
- If reviewing the SQL isn't enough, inspect the mock data provided to the unit test:
  - The parameters passed into your FeatureView function typically include temporary table names
  - You can query those temporary tables directly to see what data is available during the test.
  - If the temporary tables show less data than you'd expect, a common root cause is that Tecton filters the input data by timestamp. It filters out anything that's not within the start_time and end_time timerange unless you explicitly call ".unfiltered()" on the data source when you reference it in the FV definition's `sources` parameter.

**Detailed Debugging Examples:**

```python
@batch_feature_view(
    sources=[transactions_snowflake], 
    # ...
)
def feature_view_name(transactions, context=materialization_context()):
    # Keep explicit ISO formatting for timestamp
    end_time_iso = context.end_time.isoformat()

    # Define the SQL f-string, using single braces for source as per docs
    sql_query = f"""
    SELECT
        user_id,
        AVG(amount) AS avg_transaction_amount_30d,
        TO_TIMESTAMP('{end_time_iso}') - INTERVAL 1 MICROSECOND AS feature_timestamp
    FROM {transactions}
    WHERE transaction_time >= (TO_TIMESTAMP('{end_time_iso}') - INTERVAL 30 DAY)
      AND transaction_time < TO_TIMESTAMP('{end_time_iso}')
    GROUP BY
        user_id
    """

    print(sql_query)

    from pyspark.sql import SparkSession

    # Get the current Spark session
    spark = SparkSession.builder.getOrCreate()

    df = spark.sql(f"SELECT * FROM {transactions}")

    # Show the data
    df.show(truncate=False)

    return sql_query    
```

**Inspecting Temporary Tables:**
```python
from pyspark.sql import SparkSession

# Get the current Spark session
spark = SparkSession.builder.getOrCreate()

# Replace 'temp_table_name' with the actual name passed into your function
df = spark.sql("SELECT * FROM temp_table_name")

# Show the data
df.show(truncate=False)
```

### Configure the Local Test Spark Session

Tecton provides a Pytest session-scoped `tecton_pytest_spark_session` fixture. However, that Spark session may not be configured correctly for your tests. In that case, you may either configure the Tecton-provided fixture or create your own Spark session.

Here's an example of configuring the Tecton-provided Spark session:

```python
import pytest

@pytest.fixture(scope="module", autouse=True)
def configure_spark_session(tecton_pytest_spark_session):
    # Custom configuration for the spark session.
    tecton_pytest_spark_session.conf.set("spark.sql.session.timeZone", "UTC")
```

Here's an example of how to create your own Spark session and provide it to Tecton:

```python
from importlib import resources

@pytest.fixture(scope="session")
def my_custom_spark_session():
    """Returns a custom spark session configured for use in Tecton unit testing."""
    with resources.path("tecton_spark.jars", "tecton-udfs-spark-3.jar") as path:
        tecton_udf_jar_path = str(path)

    spark = (
        SparkSession.builder.appName("my_custom_spark_session")
        .config("spark.jars", tecton_udf_jar_path)
        # This short-circuit's Spark's attempt to auto-detect a hostname for the master address, which can lead to
        # errors on hosts with "unusual" hostnames that Spark believes are invalid.
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        tecton.set_tecton_spark_session(spark)
        yield spark
    finally:
        spark.stop()
```

### Test Examples

**Realtime Feature View Test:**
```python
def test_transaction_amount_is_high(repo_fixture: TestRepo):
    transaction_amount_is_high = repo_fixture.get_feature_view("transaction_amount_is_high")
    transaction_request = pandas.DataFrame({"amount": [124, 10001, 34235436234]})
    
    mock_context = MockContext(secrets={"my_secret": "my_secret_value"})
    actual = transaction_amount_is_high.run_transformation(
        input_data={
            "transaction_request": transaction_request,
            "context": mock_context,
        },
    ).to_pandas()
    
    expected = pandas.DataFrame({"transaction_amount_is_high": [0, 1, 1]})
    pandas.testing.assert_frame_equal(actual, expected)
```

**Batch Feature View with Spark:**
```python
def test_user_credit_card_issuer_ghf(tecton_pytest_spark_session):
    input_pandas_df = pandas.DataFrame(
        {
            "user_id": ["user_1", "user_2", "user_3", "user_4"],
            "signup_timestamp": [datetime(2022, 5, 1)] * 4,
            "cc_num": [1000000000000000, 4000000000000000, 5000000000000000, 6000000000000000],
        }
    )
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    events = pandas.DataFrame(
        {
            "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
            "timestamp": [datetime(2022, 5, 1), datetime(2022, 5, 2), datetime(2022, 6, 1), datetime(2022, 6, 1)],
        }
    )

    # Simulate materializing features for May 1st.
    output = user_credit_card_issuer.get_features_for_events(events, mock_inputs={"fraud_users_batch": input_spark_df})

    actual = output.to_pandas()

    expected = pandas.DataFrame(
        {
            "user_id": ["user_1", "user_1", "user_2", "user_not_found"],
            "timestamp": [datetime(2022, 5, 1), datetime(2022, 5, 2), datetime(2022, 6, 1), datetime(2022, 6, 1)],
            "user_credit_card_issuer__credit_card_issuer": [None, "other", "Visa", None],
        }
    )

    # NOTE: because the Spark join has non-deterministic ordering, it is important to
    # sort the dataframe to avoid test flakes.
    actual = actual.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    expected = expected.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

    pandas.testing.assert_frame_equal(actual, expected)
```

**Complete Example of Unit Test for Incremental Materialization:**
```python
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from tecton import conf, TestRepo, FeatureView, MaterializationContext

# Configure Tecton to skip online validation during tests
conf.set("TECTON_SKIP_OBJECT_VALIDATION", "True")

# Helper function to compare Spark DataFrames (handles potential ordering issues)
def assert_spark_df_equal(actual_df, expected_df):
    """ Basic comparison of Spark DataFrames by converting to Pandas """
    # Sort by key columns to ensure order doesn't affect comparison
    key_cols = ['product_category', 'store_region'] 
    
    actual_pdf = actual_df.sort(key_cols).toPandas()
    expected_pdf = expected_df.sort(key_cols).toPandas()
    
    # Basic schema check (column names)
    assert sorted(actual_pdf.columns) == sorted(expected_pdf.columns), \
        f"Column mismatch: {sorted(actual_pdf.columns)} vs {sorted(expected_pdf.columns)}"
    
    # Reorder columns for consistent comparison
    expected_pdf = expected_pdf[actual_pdf.columns]
    
    pd.testing.assert_frame_equal(actual_pdf, expected_pdf, check_dtype=False, rtol=1e-5)

@pytest.fixture(scope='module')
def spark(tecton_pytest_spark_session: SparkSession) -> SparkSession:
    """ Provides the Tecton SparkSession fixture. """
    return tecton_pytest_spark_session

def test_product_store_performance_features(repo_fixture: TestRepo, spark: SparkSession):
    """ Tests the product_store_performance_features batch feature view transformation using run_transformation. """
    
    # Get the FeatureView object from the repository fixture
    fv_under_test: FeatureView = repo_fixture.get_feature_view("product_store_performance_features")

    # Define sample input data
    transactions_data = {
        'transaction_id': [1, 2, 3, 4, 5, 6],
        'product_id': [101, 101, 102, 103, 102, 101],
        'store_id': [1, 2, 1, 1, 2, 1],
        'transaction_amount': [10.0, 15.0, 20.0, 5.0, 25.0, 12.0],
        'TRANSACTION_TIMESTAMP': [ 
            datetime(2022, 6, 15), datetime(2022, 7, 1), datetime(2022, 1, 10), 
            datetime(2022, 11, 5), datetime(2022, 12, 20), datetime(2021, 12, 30) 
        ],
        'transaction_status': ['completed', 'completed', 'completed', 'pending', 'completed', 'completed']
    }
    transactions_pdf = pd.DataFrame(transactions_data)
    transactions_pdf['TRANSACTION_TIMESTAMP'] = pd.to_datetime(transactions_pdf['TRANSACTION_TIMESTAMP'])

    # Convert to Spark DataFrames
    transactions_df = spark.createDataFrame(transactions_pdf)
    
    # Define test timeframe
    test_end_time = datetime(2023, 1, 1, 0, 0, 0)
    test_start_time = test_end_time - timedelta(days=1)
    
    # Run transformation
    actual_output_tecton_df = fv_under_test.run_transformation(
        mock_inputs={"transactions": transactions_df},
        start_time=test_start_time,
        end_time=test_end_time
    )
    
    actual_output_df = actual_output_tecton_df.to_spark()
    
    # Assertions would go here
    assert_spark_df_equal(actual_output_df, expected_output_df)
```

## Command Preferences

- Run linting and typechecking after implementation
- Use `tecton plan` for validation
- Use `tecton test` for running unit tests
- Never commit changes unless explicitly requested