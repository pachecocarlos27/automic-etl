"""Prompt templates for LLM operations."""

SCHEMA_INFERENCE_PROMPT = """You are a data engineering expert. Analyze the following sample data and infer an optimal schema.

Consider:
1. Appropriate data types (string, integer, float, boolean, date, timestamp, etc.)
2. Nullable fields
3. Primary key candidates
4. Field descriptions based on content
5. Potential relationships with other data

Sample Data:
{sample_data}

Current Column Names: {columns}

Provide the schema as JSON with this structure:
{{
    "columns": [
        {{
            "name": "column_name",
            "type": "data_type",
            "nullable": true/false,
            "description": "brief description",
            "is_primary_key": true/false,
            "suggested_rename": "new_name_if_needed"
        }}
    ],
    "table_description": "overall description",
    "suggested_partition_columns": ["col1"],
    "suggested_sort_columns": ["col1"]
}}"""

ENTITY_EXTRACTION_PROMPT = """You are an entity extraction expert. Extract structured entities from the following unstructured text.

Entity Types to Extract:
{entity_types}

Text:
{text}

For each entity found, provide:
- The entity type
- The extracted value
- Confidence score (0-1)
- The context (surrounding text)

Respond with JSON:
{{
    "entities": [
        {{
            "type": "entity_type",
            "value": "extracted_value",
            "confidence": 0.95,
            "context": "surrounding text",
            "start_pos": 0,
            "end_pos": 10
        }}
    ],
    "summary": "brief summary of extracted entities"
}}"""

DATA_CLASSIFICATION_PROMPT = """You are a data classification expert. Classify the following data based on its content and structure.

Classification Categories:
{categories}

Data Sample:
{sample_data}

Analyze the data and provide:
1. Primary classification
2. Secondary classifications (if applicable)
3. Confidence scores
4. Reasoning
5. Sensitivity level (public, internal, confidential, restricted)
6. Recommended handling

Respond with JSON:
{{
    "primary_classification": "category",
    "secondary_classifications": ["cat1", "cat2"],
    "confidence": 0.95,
    "reasoning": "explanation",
    "sensitivity_level": "internal",
    "contains_pii": true/false,
    "pii_types": ["email", "phone"],
    "recommended_handling": "description"
}}"""

QUERY_BUILDER_PROMPT = """You are a SQL query expert. Convert the following natural language query to SQL.

Available Tables:
{table_schemas}

Natural Language Query:
{natural_query}

Requirements:
1. Generate valid SQL that works with Apache Iceberg tables
2. Use appropriate JOINs if multiple tables are needed
3. Include comments explaining the query logic
4. Optimize for performance where possible

Respond with JSON:
{{
    "sql": "SELECT ...",
    "explanation": "what the query does",
    "tables_used": ["table1", "table2"],
    "estimated_complexity": "low/medium/high",
    "warnings": ["any potential issues"]
}}"""

ANOMALY_DETECTION_PROMPT = """You are a data quality expert. Analyze the following data for anomalies and issues.

Data Statistics:
{statistics}

Sample Rows:
{sample_rows}

Column Information:
{column_info}

Identify:
1. Statistical anomalies (outliers, unusual distributions)
2. Data quality issues (nulls, inconsistencies)
3. Pattern violations
4. Potential data entry errors
5. Business rule violations

Respond with JSON:
{{
    "anomalies": [
        {{
            "type": "anomaly_type",
            "column": "column_name",
            "description": "what's wrong",
            "severity": "low/medium/high",
            "affected_rows_estimate": 100,
            "recommendation": "how to fix"
        }}
    ],
    "overall_quality_score": 0.85,
    "summary": "overall assessment"
}}"""

DATA_QUALITY_PROMPT = """You are a data quality analyst. Evaluate the quality of the following dataset.

Dataset Information:
- Row Count: {row_count}
- Column Count: {column_count}
- Columns: {columns}

Data Profile:
{data_profile}

Sample Data:
{sample_data}

Evaluate:
1. Completeness (missing values)
2. Consistency (format consistency)
3. Accuracy (likely correct values)
4. Timeliness (date freshness)
5. Uniqueness (duplicates)
6. Validity (domain constraints)

Respond with JSON:
{{
    "quality_dimensions": {{
        "completeness": {{"score": 0.95, "issues": []}},
        "consistency": {{"score": 0.90, "issues": []}},
        "accuracy": {{"score": 0.85, "issues": []}},
        "timeliness": {{"score": 0.80, "issues": []}},
        "uniqueness": {{"score": 0.99, "issues": []}},
        "validity": {{"score": 0.92, "issues": []}}
    }},
    "overall_score": 0.90,
    "critical_issues": [],
    "recommendations": [],
    "fit_for_purpose": true/false
}}"""

TRANSFORMATION_SUGGESTION_PROMPT = """You are a data transformation expert. Suggest transformations to clean and prepare this data.

Source Schema:
{source_schema}

Sample Data:
{sample_data}

Target Use Case: {use_case}

Suggest transformations for:
1. Data cleaning (handle nulls, fix formats)
2. Standardization (dates, currencies, units)
3. Enrichment (derived columns)
4. Normalization (column names, values)

Respond with JSON:
{{
    "transformations": [
        {{
            "column": "column_name",
            "operation": "operation_type",
            "parameters": {{}},
            "reason": "why this transformation",
            "priority": 1
        }}
    ],
    "new_columns": [
        {{
            "name": "new_col",
            "expression": "how to derive",
            "description": "what it represents"
        }}
    ],
    "columns_to_drop": ["col1"],
    "column_renames": {{"old": "new"}}
}}"""
