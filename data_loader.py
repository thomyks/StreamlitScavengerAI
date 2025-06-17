"""
Data Loader Module - Component One
Fast CSV processing with Polars for better performance than Pandas
"""

import polars as pl
import os
import time
import random
from pathlib import Path
from typing import List, Dict, Tuple, Any


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"


def analyze_file(file_path: Path, sample_lines: int = 5, use_random=False, max_random_lines=5000) -> Dict:
    """Analyze CSV file structure to detect delimiter and columns"""
    print(f"🔍 Analyzing: {file_path.name}")
    lines = []

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        all_lines = [line.strip() for _, line in zip(range(max_random_lines), f)]

    # Filter out empty or comment lines
    all_lines = [l for l in all_lines if l and not l.startswith("#")]

    if use_random:
        lines = random.sample(all_lines, min(len(all_lines), sample_lines))
    else:
        lines = all_lines[:sample_lines]

    if not lines:
        return {'error': 'empty_file'}

    first_line = lines[0]
    delimiters = [',', ';', '\t', '|']
    counts = {d: first_line.count(d) for d in delimiters}
    delimiter = max(counts, key=counts.get)
    expected_cols = counts[delimiter] + 1

    return {
        'delimiter': delimiter,
        'expected_columns': expected_cols,
        'sample_lines': lines,
        'likely_header': first_line.split(delimiter)
    }


def load_csv_fast(file_path: Path, delimiter: str) -> pl.DataFrame:
    """Load CSV using Polars for better performance than Pandas"""
    try:
        df = pl.read_csv(
            file_path, 
            separator=delimiter,
            ignore_errors=True,
            encoding="utf8-lossy",
            infer_schema_length=100
        )
        return df
    except Exception as e:
        print(f"❌ Failed: {e}")
        return None


def fix_merged_columns(df: pl.DataFrame, file_analysis: Dict) -> pl.DataFrame:
    """Fix columns that were merged due to incorrect delimiter detection"""
    if df.shape[1] != 1:
        return df
    
    col = df.columns[0]
    data = df[col].to_list()
    rows = [row.split(file_analysis['delimiter']) for row in data if row]
    
    headers = file_analysis['likely_header']
    max_cols = max(len(row) for row in rows)
    while len(headers) < max_cols:
        headers.append(f"COL_{len(headers)+1}")
    
    col_data = {h: [r[i] if i < len(r) else None for r in rows] for i, h in enumerate(headers)}
    return pl.DataFrame(col_data)


def clean_string_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Clean string columns by removing extra whitespace"""
    string_cols = [c for c in df.columns if df[c].dtype == pl.Utf8]
    if not string_cols:
        return df
    
    expressions = [
        pl.col(c).str.strip_chars().replace("", None).alias(c) if c in string_cols else pl.col(c)
        for c in df.columns
    ]
    return df.select(expressions)


def process_csv_file(file_path: str) -> pl.DataFrame:
    """Main function to process CSV file with error handling"""
    file_path = Path(file_path)
    analysis = analyze_file(file_path)
    
    if 'error' in analysis:
        print(f"❌ Skipping {file_path.name}: {analysis['error']}")
        return None
    
    df = load_csv_fast(file_path, analysis['delimiter'])
    if df is None:
        return None
    
    # Fix merged column if needed
    if df.shape[1] == 1 and analysis['delimiter'] in df.columns[0]:
        df = fix_merged_columns(df, analysis)
    
    df = clean_string_columns(df)
    return df


def load_data_file(uploaded_file) -> pl.DataFrame:
    """Load data from Streamlit uploaded file using Polars"""
    try:
        if uploaded_file.name.endswith('.csv'):
            # For CSV files, use our optimized Polars loader
            # Save uploaded file temporarily
            temp_path = Path(f"/tmp/{uploaded_file.name}")
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getvalue())
            
            # Process with our optimized function
            df = process_csv_file(str(temp_path))
            
            # Clean up temp file
            os.unlink(temp_path)
            
            return df
        else:
            # For Excel files, convert from pandas to polars
            import pandas as pd
            pandas_df = pd.read_excel(uploaded_file)
            return pl.from_pandas(pandas_df)
            
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None


def load_multiple_files(uploaded_files) -> Dict[str, pl.DataFrame]:
    """Load multiple data files and return dictionary of DataFrames
    
    Args:
        uploaded_files: List of Streamlit uploaded file objects
        
    Returns:
        Dictionary mapping filenames to Polars DataFrames
    """
    results = {}
    start_time = time.time()
    total_files = len(uploaded_files)
    
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            print(f"Processing file {i+1}/{total_files}: {uploaded_file.name}")
            df = load_data_file(uploaded_file)
            if df is not None:
                results[uploaded_file.name] = df
                print(f"✅ Successfully loaded: {uploaded_file.name} ({df.shape[0]} rows × {df.shape[1]} columns)")
            else:
                print(f"❌ Failed to load {uploaded_file.name}")
        except Exception as e:
            print(f"❌ Error loading {uploaded_file.name}: {e}")
    
    elapsed = time.time() - start_time
    print(f"🏁 Completed loading {len(results)}/{total_files} files in {format_duration(elapsed)}")
    
    return results
