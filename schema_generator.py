"""
Schema Generator Module - Component Three
AI-powered SQL schema generation with Claude API
"""

import json
import requests
import time
import polars as pl
import os
from typing import Dict, List, Tuple, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Claude API configuration
CLAUDE_API_KEY = os.getenv('CLAUDE_API_KEY')
CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"

# Validate API key is present
if not CLAUDE_API_KEY:
    raise ValueError(
        "🔑 CLAUDE_API_KEY environment variable is not set!\n"
        "Please set it using: export CLAUDE_API_KEY='your-api-key-here'\n"
        "Or create a .env file with CLAUDE_API_KEY=your-api-key-here"
    )


def extract_column_samples(df: pl.DataFrame, sample_size: int = 5) -> Dict[str, List[Any]]:
    """Extract sample values from each column for LLM analysis"""
    samples = {}
    
    for col in df.columns:
        # Get non-null values first
        non_null_values = df[col].drop_nulls()
        
        if non_null_values.len() > 0:
            # Take sample_size random samples (or all if less than sample_size)
            sample_count = min(sample_size, non_null_values.len())
            if sample_count == non_null_values.len():
                samples[col] = non_null_values.to_list()
            else:
                samples[col] = non_null_values.sample(sample_count).to_list()
        else:
            samples[col] = ["NULL"]  # Indicate all nulls
    
    return samples


def prepare_claude_prompt(column_samples: Dict[str, List[Any]], table_name: str) -> str:
    """Prepare enhanced prompt for Claude API with column samples"""
    
    # Build the column analysis section
    column_analysis = ""
    for col_name, samples in column_samples.items():
        column_analysis += f"""--- {col_name} ---
Sample Values: {samples}

"""
    
    prompt = f"""You are a highly skilled data analyst and SQL schema designer. Your task is to generate **precise, professional column-level metadata** for a structured tabular dataset. This metadata will support accurate SQL schema generation and human-readable documentation.

### Dataset Context:
- Table name: `{table_name}`
- Each column is defined by:
  - A column name
  - A small set of sample values

### Your Output Format:
Return a structured JSON object with one entry per column under a `columns` key. For each column, provide:

- `sql_type`: SQL-compatible data type (e.g., `INT`, `FLOAT`, `VARCHAR(n)`, `BOOLEAN`, `DATE`) — optimized for **MySQL**.
- `nullable`: Boolean (`true` or `false`) — inferred from presence of null values or typical business logic.
- `examples`: The provided sample values (unaltered).
- `description`: A **single, human-readable description (max 400 characters)** that clearly explains what the column is, including its purpose, typical values, or business meaning. The description should make it easy for AI to later generate correct SQL using the schema.

### Critical Requirements:
- **BE DEFINITIVE**: Use assertive language. Avoid words like "likely", "may", "appears", "could be", "seems", "probably"
- **NO INVESTIGATION NEEDED**: Never say "needs investigation" or "requires further analysis" 
- **BUSINESS FOCUS**: Explain the business purpose, not just technical details
- **DESCRIPTIVE**: Include purpose, typical values, and business meaning when apparent
- **SQL-FRIENDLY**: Write descriptions that help AI understand the column for SQL generation
- **CONCISE**: Keep descriptions under 400 characters but be comprehensive

### Formatting Instructions:
- For `VARCHAR(n)`, estimate `n` based on the longest example string plus a buffer (minimum 20, maximum 500).
- Date fields: Use appropriate MySQL date types (DATE, DATETIME, TIMESTAMP)
- Monetary fields: Use DECIMAL(10,2) for currency amounts
- Flags/indicators: Use BOOLEAN or VARCHAR(1) as appropriate

### Output Template:
{{
  "columns": {{
    "column_name": {{
      "sql_type": "VARCHAR(50)",
      "nullable": false,
      "examples": ["example1", "example2", "example3"],
      "description": "Comprehensive explanation of what this column represents, its business purpose, typical values, and how it's used in business processes - designed to help AI understand the column for accurate SQL generation"
    }}
  }}
}}

## Few-shot example

Input:
--- age ---
Sample Values: [25, 42, 60]

--- smoker ---
Sample Values: ["yes", "no", "no"]

--- charges ---
Sample Values: [16884.92, 1725.55, 4449.46]

Expected Output:
{{
  "columns": {{
    "age": {{
      "sql_type": "INT",
      "nullable": false,
      "examples": [25, 42, 60],
      "description": "Age of the insured individual in years, used for risk assessment and premium calculation. Values typically range from 18-80. Critical for actuarial analysis and determining insurance eligibility and pricing tiers."
    }},
    "smoker": {{
      "sql_type": "VARCHAR(3)",
      "nullable": false,
      "examples": ["yes", "no", "no"],
      "description": "Smoking status indicator showing whether the individual is a current smoker. Values are 'yes' for smokers and 'no' for non-smokers. Directly impacts health risk assessment and premium calculations in insurance policies."
    }},
    "charges": {{
      "sql_type": "DECIMAL(10,2)",
      "nullable": false,
      "examples": [16884.92, 1725.55, 4449.46],
      "description": "Total medical expenses or insurance charges in dollars, representing the monetary amount billed to insurance or paid by policyholder. Used for financial reporting, claims analysis, and actuarial modeling."
    }}
  }}
}}

## Now analyze the following input:
{column_analysis}"""
    
    return prompt


def call_claude_api_robust(prompt: str, max_retries: int = 3, model: str = "claude-sonnet-4-20250514") -> Dict:
    """Enhanced Claude API call with retries, timeouts, and exponential backoff"""
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01"
    }
    
    data = {
        "model": model,
        "max_tokens": 4000,
        "temperature": 0.1,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
    
    for attempt in range(max_retries):
        try:
            print(f"📡 Attempt {attempt + 1}/{max_retries}: Calling Claude API...")
            
            response = requests.post(
                CLAUDE_API_URL, 
                headers=headers, 
                json=data, 
                timeout=120
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["content"][0]["text"]
            
            # Handle markdown code blocks
            if "```json" in content:
                content = content.split("```json")[1]
                if "```" in content:
                    content = content.split("```")[0]
            elif "```" in content and content.count("```") >= 2:
                parts = content.split("```")
                if len(parts) >= 3:
                    content = parts[1]
            
            # Extract JSON from the response
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            
            if start_idx != -1 and end_idx > start_idx:
                json_str = content[start_idx:end_idx].strip()
                parsed_json = json.loads(json_str)
                print(f"✅ Successfully parsed response on attempt {attempt + 1}")
                return parsed_json
            else:
                print(f"⚠️ Attempt {attempt + 1}: Could not find JSON in Claude response")
                if attempt == max_retries - 1:
                    print(f"Raw content preview: {content[:200]}...")
                    return None
                    
        except requests.exceptions.Timeout:
            print(f"⏰ Attempt {attempt + 1}: Request timeout (120s exceeded)")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                
        except requests.exceptions.ConnectionError:
            print(f"🌐 Attempt {attempt + 1}: Connection error")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                
        except requests.exceptions.HTTPError as e:
            print(f"🚫 Attempt {attempt + 1}: HTTP error {e.response.status_code}")
            if e.response.status_code == 429:  # Rate limit
                if attempt < max_retries - 1:
                    wait_time = 10 + (2 ** attempt)
                    print(f"🚦 Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
            elif e.response.status_code >= 500:  # Server error
                if attempt < max_retries - 1:
                    wait_time = 5 + (2 ** attempt)
                    print(f"🔧 Server error. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
            else:
                print(f"❌ Non-retryable HTTP error: {e}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"📄 Attempt {attempt + 1}: JSON parse error - {e}")
            if attempt == max_retries - 1:
                print(f"Final JSON content preview: {content[:300] if 'content' in locals() else 'No content'}...")
                return None
                
        except Exception as e:
            print(f"❌ Attempt {attempt + 1}: Unexpected error - {e}")
            if attempt == max_retries - 1:
                import traceback
                print(f"Full traceback: {traceback.format_exc()}")
                return None
    
    print(f"💥 All {max_retries} attempts failed")
    return None


def generate_ddl_from_schema(schema_data: Dict, table_name: str) -> str:
    """Generate CREATE TABLE DDL from Claude schema analysis"""
    
    if not schema_data or 'columns' not in schema_data:
        return "-- Error: Invalid schema data"
    
    ddl = f"CREATE TABLE {table_name} (\n"
    
    column_definitions = []
    for col_name, col_info in schema_data['columns'].items():
        sql_type = col_info.get('sql_type', 'VARCHAR(255)')
        nullable = col_info.get('nullable', True)
        
        null_constraint = " NOT NULL" if not nullable else ""
        column_def = f"  `{col_name}` {sql_type}{null_constraint}"
        column_definitions.append(column_def)
    
    ddl += ",\n".join(column_definitions)
    ddl += "\n);"
    
    return ddl


def generate_column_descriptions(schema_data: Dict) -> str:
    """Generate JSON formatted column descriptions"""
    
    if not schema_data or 'columns' not in schema_data:
        return "{}"
    
    descriptions = {}
    for col_name, col_info in schema_data['columns'].items():
        descriptions[col_name] = col_info.get('description', f"Description for {col_name}")
    
    return json.dumps(descriptions, indent=2)


def analyze_schema_with_claude(df: pl.DataFrame, table_name: str, sample_size: int = 5) -> Tuple[str, str]:
    """Complete pipeline: extract samples, analyze with Claude, generate DDL and descriptions"""
    
    print(f"🔍 Analyzing schema for {table_name} using Claude...")
    
    # Step 1: Extract column samples
    column_samples = extract_column_samples(df, sample_size)
    print(f"✔️ Extracted samples from {len(column_samples)} columns")
    
    # Step 2: Prepare Claude prompt
    prompt = prepare_claude_prompt(column_samples, table_name)
    
    # Step 3: Call Claude for analysis with robust error handling
    print("🤖 Calling Claude API for schema analysis...")
    schema_data = call_claude_api_robust(prompt)
    
    if not schema_data:
        return "-- Error: Failed to generate schema after multiple attempts", "{}"
    
    # Step 4: Generate DDL and descriptions
    ddl = generate_ddl_from_schema(schema_data, table_name)
    descriptions = generate_column_descriptions(schema_data)
    
    print(f"✅ Schema analysis completed for {table_name}")
    
    return ddl, descriptions


def generate_schema_with_auto_batching(df: pl.DataFrame, table_name: str) -> Tuple[str, str]:
    """
    Generate schema with automatic batching for datasets.
    - Small datasets (≤20 columns): Single batch processing
    - Large datasets (>20 columns): Automatic 20-column batches
    """
    
    total_columns = len(df.columns)
    BATCH_SIZE = 20
    
    print(f"📊 Dataset '{table_name}': {df.shape[0]:,} rows × {total_columns} columns")
    
    if total_columns <= BATCH_SIZE:
        # Small dataset - process all at once
        print(f"✅ Small dataset detected. Processing all {total_columns} columns in single batch.")
        return analyze_schema_with_claude(df, table_name, sample_size=5)
    
    else:
        # Large dataset - process in batches of 20
        num_batches = (total_columns + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"📦 Large dataset detected. Processing in {num_batches} batches of {BATCH_SIZE} columns each.")
        
        all_schemas = {}
        
        for batch_num in range(1, num_batches + 1):
            start_idx = (batch_num - 1) * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_columns)
            batch_columns = df.columns[start_idx:end_idx]
            
            print(f"\n🔄 Batch {batch_num}/{num_batches}: Processing columns {start_idx + 1}-{end_idx}")
            
            # Create batch DataFrame
            batch_df = df.select(batch_columns)
            
            # Extract samples for this batch
            batch_samples = extract_column_samples(batch_df, sample_size=5)
            
            # Create prompt for this batch
            batch_prompt = prepare_claude_prompt(batch_samples, f"{table_name}_batch_{batch_num}")
            
            # Call Claude API
            print(f"   🤖 Calling Claude API...")
            batch_schema = call_claude_api_robust(batch_prompt)
            
            if batch_schema and 'columns' in batch_schema:
                all_schemas.update(batch_schema['columns'])
                print(f"   ✅ Batch {batch_num} completed successfully ({len(batch_schema['columns'])} columns processed)")
            else:
                print(f"   ❌ Batch {batch_num} failed - skipping")
        
        # Combine all schemas
        print(f"\n🔗 Combining results from all batches...")
        combined_schema = {'columns': all_schemas}
        ddl = generate_ddl_from_schema(combined_schema, table_name)
        descriptions = generate_column_descriptions(combined_schema)
        
        print(f"✅ Successfully processed {len(all_schemas)} columns across {num_batches} batches")
        return ddl, descriptions


def test_claude_connection() -> bool:
    """Test if Claude API is accessible"""
    test_prompt = "Please respond with just the word 'SUCCESS' if you can read this."
    
    # Get the default model from call_claude_api_robust function
    default_model = call_claude_api_robust.__defaults__[1]
    
    print(f"🔗 Testing {default_model} API connection...")
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01"
    }
    
    data = {
        "model": default_model,
        "max_tokens": 10,
        "messages": [{"role": "user", "content": test_prompt}]
    }
    
    try:
        response = requests.post(CLAUDE_API_URL, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        result = response.json()
        content = result["content"][0]["text"]
        
        if "SUCCESS" in content:
            print("✅ Claude API connection successful")
            return True
        else:
            print(f"⚠️ Claude API responded but with unexpected content: {content}")
            return False
            
    except Exception as e:
        print(f"❌ Claude API connection failed: {e}")
        return False


def format_for_copy_paste(ddl: str, descriptions: str, table_name: str) -> str:
    """Format results for easy copy-paste with timestamps and metadata"""
    
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    output = f"""-- =========================================
-- SCAVENGER AI - GENERATED SCHEMA
-- =========================================
-- Table Name: {table_name.upper()}
-- Generated: {timestamp}
-- Tool: Scavenger AI Schema Generator v1.0
-- =========================================

{ddl}

-- =========================================
-- COLUMN DESCRIPTIONS
-- =========================================
/*
Generated Column Descriptions (JSON Format):

{descriptions}
*/

-- =========================================
-- IMPLEMENTATION NOTES
-- =========================================
/*
1. Review data types for optimization
2. Add indexes as needed for performance
3. Consider adding primary key constraints
4. Validate NOT NULL constraints with business rules
5. Add foreign key relationships if applicable
*/
"""
    return output


def prepare_enhanced_claude_prompt_with_descriptions(
    column_samples: Dict[str, List[Any]], 
    table_name: str,
    existing_descriptions: Dict[str, str] = None
) -> str:
    """Enhanced prompt that uses pre-generated descriptions for better type inference"""
    
    prompt = f"""You are a database schema expert. Generate optimized MySQL schema using provided column samples AND business descriptions.

Dataset: {table_name}

IMPORTANT: Use the business descriptions to make more accurate data type decisions. Consider these patterns:
- If description mentions "identifier", "ID", "key", "reference" → Use appropriate INT/BIGINT
- If description mentions "amount", "price", "cost", "value", "money" → Use DECIMAL(10,2)
- If description mentions "date", "time", "timestamp", "created", "updated" → Use DATE/DATETIME/TIMESTAMP
- If description mentions "status", "flag", "indicator", "active", "enabled" → Consider BOOLEAN
- If description mentions "code", "category", "type", "name" → Use VARCHAR with appropriate length
- If description mentions "email" → Use VARCHAR(255)
- If description mentions "phone" → Use VARCHAR(20)
- If description mentions "percentage", "rate", "ratio" → Use DECIMAL(5,2)

Column Analysis:
"""
    
    for col_name, samples in column_samples.items():
        business_desc = existing_descriptions.get(col_name, "No description available") if existing_descriptions else "No description available"
        
        prompt += f"""
--- {col_name} ---
Business Description: {business_desc}
Sample Values: {samples}
Data Pattern Analysis: Look at both the business context AND sample values to choose optimal type
"""
    
    prompt += """

Provide JSON format with enhanced type reasoning:
{
  "columns": {
    "column_name": {
      "sql_type": "MySQL type informed by business context and data patterns",
      "nullable": true/false,
      "description": "Enhanced description explaining the type choice rationale",
      "type_reasoning": "Brief explanation of why this SQL type was chosen"
    }
  }
}

Guidelines:
- Prioritize business context over raw data patterns
- Use appropriate constraints (NOT NULL for critical fields)
- Consider future scalability in type choices
- Explain type reasoning for transparency
"""
    return prompt


def generate_descriptions_only(df: pl.DataFrame, table_name: str, sample_size: int = 5) -> Dict[str, str]:
    """Generate business descriptions with enhanced assertive prompt structure"""
    
    print(f"🔍 Generating business descriptions for {table_name}...")
    
    # Extract column samples
    column_samples = extract_column_samples(df, sample_size)
    
    # Detect domain context for better prompting
    domain_context = detect_business_domain(table_name)
    
    # Create enhanced business-focused prompt
    desc_prompt = f"""You are a senior business analyst and domain expert specializing in {domain_context}. Your task is to generate definitive, professional column descriptions that explain business value and operational significance.

### Dataset Context:
- Table: `{table_name}`
- Domain: {domain_context}
- Purpose: Generate business-focused metadata for stakeholders and analysts

### Column Analysis Rules:
1. **BE DEFINITIVE**: Use confident, assertive language
2. **BUSINESS PURPOSE**: Focus on why this data matters to the business
3. **NO SPECULATION**: Avoid "likely", "may", "appears", "needs investigation"
4. **CONCISE**: 80-200 characters per description
5. **ACTIONABLE**: Explain how this data supports business decisions

### Domain-Specific Guidelines:
{get_domain_guidelines(table_name)}

### Expected Output Format:
{{
  "columns": {{
    "column_name": {{
      "sql_type": "MySQL-compatible type",
      "nullable": true/false,
      "examples": [actual_samples],
      "description": "Definitive business explanation (80-200 chars)"
    }}
  }}
}}

### Business-Focused Examples:

For SAP Sales Data:
- VBELN → "Sales document number enabling order tracking and customer service inquiries"
- WAERK → "Transaction currency determining pricing calculations and financial reporting"
- NETWR → "Net order value for revenue recognition and commission calculations"

For CRM Data:
- CUST_ID → "Customer identifier enabling relationship tracking and service history access"
- STATUS → "Account status controlling access permissions and service eligibility"

For Financial Data:
- AMOUNT → "Transaction value for accounting entries and financial statement preparation"
- GL_ACCOUNT → "General ledger code for expense categorization and budget tracking"

### Column Data to Analyze:
"""
    
    for col_name, samples in column_samples.items():
        desc_prompt += f"""
--- {col_name} ---
Sample Values: {samples}
"""
    
    desc_prompt += """

### Instructions:
1. Analyze each column name and sample values
2. Determine the business function and operational purpose
3. Write descriptions that explain business value, not just data structure
4. Use active, confident language
5. Focus on how business users would understand and use this data

Generate definitive business descriptions now:"""
    
    # Call Claude API
    result = call_claude_api_robust(desc_prompt)
    
    if result and 'columns' in result:
        # Extract descriptions from the columns structure
        descriptions = {col: info.get('description', f"Business field for {col}") 
                       for col, info in result['columns'].items()}
        print(f"✅ Generated descriptions for {len(descriptions)} columns")
        return descriptions
    elif result and 'descriptions' in result:
        # Alternative format - direct descriptions object
        print(f"✅ Generated descriptions for {len(result['descriptions'])} columns")
        return result['descriptions']
    else:
        print("❌ Failed to generate descriptions")
        print(f"Debug: Result keys: {list(result.keys()) if result else 'None'}")
        return {}


def generate_enhanced_schema_with_descriptions(
    df: pl.DataFrame, 
    table_name: str, 
    descriptions: Dict[str, str],
    sample_size: int = 5
) -> Tuple[str, str]:
    """Generate schema using both data samples AND business descriptions for better type inference"""
    
    print(f"🔍 Generating enhanced schema for {table_name} using business descriptions...")
    
    # Extract column samples
    column_samples = extract_column_samples(df, sample_size)
    
    # Create enhanced prompt with descriptions
    prompt = prepare_enhanced_claude_prompt_with_descriptions(column_samples, table_name, descriptions)
    
    # Call Claude API
    schema_data = call_claude_api_robust(prompt)
    
    if not schema_data or 'columns' not in schema_data:
        return "-- Error: Failed to generate enhanced schema", "{}"
    
    # Generate DDL and enhanced descriptions
    ddl = generate_ddl_from_schema(schema_data, table_name)
    enhanced_descriptions = generate_column_descriptions(schema_data)
    
    print(f"✅ Enhanced schema generated for {table_name}")
    return ddl, enhanced_descriptions


def generate_enhanced_schema_with_auto_batching(
    df: pl.DataFrame, 
    table_name: str, 
    descriptions: Dict[str, str],
    sample_size: int = 5
) -> Tuple[str, str]:
    """
    Generate enhanced schema with automatic batching for large datasets.
    - Small datasets (≤20 columns): Single batch processing
    - Large datasets (>20 columns): Automatic 20-column batches
    
    This combines the benefits of batch processing and enhanced descriptions.
    """
    
    total_columns = len(df.columns)
    BATCH_SIZE = 20
    
    print(f"📊 Enhanced Schema for '{table_name}': {df.shape[0]:,} rows × {total_columns} columns")
    
    if total_columns <= BATCH_SIZE:
        # Small dataset - process all at once
        print(f"✅ Small dataset detected. Processing all {total_columns} columns in single batch.")
        return generate_enhanced_schema_with_descriptions(df, table_name, descriptions, sample_size)
    
    else:
        # Large dataset - process in batches of 20
        num_batches = (total_columns + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"📦 Large dataset detected. Processing in {num_batches} batches of {BATCH_SIZE} columns each.")
        
        all_schemas = {}
        
        for batch_num in range(1, num_batches + 1):
            start_idx = (batch_num - 1) * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_columns)
            batch_columns = df.columns[start_idx:end_idx]
            
            print(f"\n🔄 Batch {batch_num}/{num_batches}: Processing columns {start_idx + 1}-{end_idx}")
            
            # Create batch DataFrame
            batch_df = df.select(batch_columns)
            
            # Get relevant descriptions for this batch
            batch_descriptions = {col: descriptions.get(col, f"Column {col}") for col in batch_columns}
            
            # Extract samples for this batch
            batch_samples = extract_column_samples(batch_df, sample_size)
            
            # Create enhanced prompt with descriptions for this batch
            batch_prompt = prepare_enhanced_claude_prompt_with_descriptions(
                batch_samples, 
                f"{table_name}_batch_{batch_num}", 
                batch_descriptions
            )
            
            # Call Claude API
            print(f"   🤖 Calling Claude API...")
            batch_schema = call_claude_api_robust(batch_prompt)
            
            if batch_schema and 'columns' in batch_schema:
                all_schemas.update(batch_schema['columns'])
                print(f"   ✅ Batch {batch_num} completed successfully ({len(batch_schema['columns'])} columns processed)")
            else:
                print(f"   ❌ Batch {batch_num} failed - skipping")
                
                # Try one more time with a different approach
                print(f"   🔄 Retrying batch {batch_num} with standard approach...")
                standard_prompt = prepare_claude_prompt(batch_samples, f"{table_name}_batch_{batch_num}")
                batch_schema = call_claude_api_robust(standard_prompt)
                
                if batch_schema and 'columns' in batch_schema:
                    all_schemas.update(batch_schema['columns'])
                    print(f"   ✅ Batch {batch_num} completed successfully with standard approach")
                else:
                    print(f"   ❌ Batch {batch_num} failed again - skipping")
        
        # Combine all schemas
        print(f"\n🔗 Combining results from all batches...")
        combined_schema = {'columns': all_schemas}
        ddl = generate_ddl_from_schema(combined_schema, table_name)
        enhanced_descriptions = generate_column_descriptions(combined_schema)
        
        print(f"✅ Successfully processed {len(all_schemas)} columns across {num_batches} batches")
        return ddl, enhanced_descriptions


def detect_business_domain(table_name: str) -> str:
    """Detect business domain from table name for better prompt context"""
    table_lower = table_name.lower()
    
    if any(term in table_lower for term in ['sap', 'vbak', 'vbap', 'ekko', 'ekpo']):
        return "SAP ERP systems"
    elif any(term in table_lower for term in ['crm', 'customer', 'contact', 'lead']):
        return "customer relationship management (CRM)"
    elif any(term in table_lower for term in ['sales', 'order', 'invoice', 'billing']):
        return "sales and distribution"
    elif any(term in table_lower for term in ['finance', 'accounting', 'ledger', 'payment']):
        return "financial management"
    elif any(term in table_lower for term in ['inventory', 'warehouse', 'stock', 'material']):
        return "inventory and supply chain management"
    elif any(term in table_lower for term in ['hr', 'employee', 'payroll', 'staff']):
        return "human resources management"
    else:
        return "business data management"


def get_domain_guidelines(table_name: str) -> str:
    """Get domain-specific guidelines for description generation"""
    table_lower = table_name.lower()
    
    if any(term in table_lower for term in ['sap', 'vbak', 'vbap']):
        return """
For SAP fields:
- Focus on business process flow (order-to-cash, procure-to-pay)
- Explain impact on financial reporting and compliance
- Mention integration points with other SAP modules
- Use SAP terminology (document numbers, organizational units, etc.)
"""
    elif any(term in table_lower for term in ['crm', 'customer']):
        return """
For CRM fields:
- Emphasize customer relationship and service impact
- Explain how data supports sales and marketing processes
- Focus on customer experience and retention value
- Mention data quality importance for customer insights
"""
    elif any(term in table_lower for term in ['finance', 'accounting']):
        return """
For Financial fields:
- Emphasize compliance and audit requirements
- Explain impact on financial statements and reporting
- Focus on control and risk management aspects
- Mention regulatory and tax implications
"""
    else:
        return """
General business guidelines:
- Focus on operational efficiency and decision-making support
- Explain how data drives business processes and outcomes
- Emphasize data quality importance for analytics and reporting
- Connect technical data to business value and user needs
"""
