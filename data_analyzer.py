"""
Data Analysis Module - Component Two
Comprehensive data quality analysis and descriptive statistics
"""

import polars as pl
from typing import Dict, List, Tuple


def analyze_dataset_structure_and_nulls(df: pl.DataFrame, name: str) -> Dict:
    """Comprehensive analysis of dataset structure and data quality"""
    
    analysis_results = {
        'name': name,
        'shape': df.shape,
        'memory_mb': df.estimated_size('mb'),
        'columns': {},
        'summary': {}
    }
    
    print(f"\n📊 {name.upper()}")
    print("─" * 60)
    
    # Shape and Memory
    print(f"✔️  Rows × Columns:       {df.shape[0]} × {df.shape[1]}")
    print(f"✔️  Memory Usage:         {df.estimated_size('mb'):.1f} MB")

    # Check for delimiter issue
    if df.shape[1] == 1:
        print("⚠️  Warning: Only one column detected — possible delimiter issue.")
        analysis_results['warnings'] = ['single_column_detected']
    else:
        print(f"✔️  Column Count OK:      {df.shape[1]} columns")

    # Column overview with enhanced statistics
    print("\n📑 Column Analysis:")
    numeric_cols = []
    string_cols = []
    
    for i, col in enumerate(df.columns):
        try:
            null_count = df[col].null_count()
            null_pct = (null_count / df.shape[0]) * 100
            dtype = str(df[col].dtype)
            
            print(f"  {i+1:>2}. {col:<25} {dtype:<12}  Nulls: {null_pct:5.1f}% ({null_count:,})")
            
            # Store column info
            analysis_results['columns'][col] = {
                'dtype': dtype,
                'null_count': null_count,
                'null_percentage': null_pct,
                'non_null_count': df.shape[0] - null_count
            }
            
            # Collect column types for detailed stats
            if df[col].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                numeric_cols.append(col)
            elif df[col].dtype == pl.Utf8:
                string_cols.append(col)
                
            if null_pct > 50:
                print(f"      ⚠️ Over 50% missing values in '{col}'")
                analysis_results['columns'][col]['warning'] = 'high_null_percentage'
                
        except Exception as e:
            print(f"      ❌ Error analyzing column '{col}': {e}")
            analysis_results['columns'][col] = {'error': str(e)}

    # Analyze numeric columns
    if numeric_cols:
        analysis_results['numeric_columns'] = analyze_numeric_columns(df, numeric_cols)
    
    # Analyze string columns  
    if string_cols:
        analysis_results['string_columns'] = analyze_string_columns(df, string_cols)

    # Overall Data Quality Summary
    total_cells = df.shape[0] * df.shape[1]
    total_nulls = sum(df[col].null_count() for col in df.columns)
    completeness = ((total_cells - total_nulls) / total_cells) * 100
    
    analysis_results['summary'] = {
        'total_cells': total_cells,
        'total_nulls': total_nulls,
        'completeness_percentage': completeness,
        'numeric_columns_count': len(numeric_cols),
        'string_columns_count': len(string_cols)
    }
    
    print(f"\n📋 DATA QUALITY SUMMARY")
    print("─" * 40)
    print(f"✔️  Total cells:          {total_cells:,}")
    print(f"✔️  Non-null cells:       {total_cells - total_nulls:,}")
    print(f"✔️  Data completeness:    {completeness:.1f}%")
    print(f"✔️  Numeric columns:      {len(numeric_cols)}")
    print(f"✔️  String columns:       {len(string_cols)}")

    return analysis_results


def analyze_numeric_columns(df: pl.DataFrame, numeric_cols: List[str]) -> Dict:
    """Detailed analysis of numeric columns"""
    
    print(f"\n📈 NUMERIC COLUMNS STATISTICS ({len(numeric_cols)} columns)")
    print("─" * 80)
    
    numeric_analysis = {}
    
    for col in numeric_cols:
        try:
            non_null_count = df.shape[0] - df[col].null_count()
            
            # Calculate statistics
            col_stats = {
                'count': non_null_count,
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'q25': df[col].quantile(0.25),
                'median': df[col].median(),
                'q75': df[col].quantile(0.75),
                'max': df[col].max()
            }
            
            print(f"\n🔢 {col.upper()}")
            print(f"   Count (non-null): {non_null_count:,}")
            print(f"   Mean:            {col_stats['mean']:.4f}")
            print(f"   Std Dev:         {col_stats['std']:.4f}")
            print(f"   Min:             {col_stats['min']}")
            print(f"   25th Percentile: {col_stats['q25']}")
            print(f"   Median (50th):   {col_stats['median']}")
            print(f"   75th Percentile: {col_stats['q75']}")
            print(f"   Max:             {col_stats['max']}")
            
            # Check for outliers using IQR method
            q1 = col_stats['q25']
            q3 = col_stats['q75']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = df.filter((pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)).shape[0]
            
            col_stats['outliers_count'] = outliers
            col_stats['outlier_bounds'] = (lower_bound, upper_bound)
            
            if outliers > 0:
                print(f"   ⚠️ Potential outliers: {outliers:,} values outside [{lower_bound:.2f}, {upper_bound:.2f}]")
            
            numeric_analysis[col] = col_stats
                
        except Exception as e:
            print(f"   ❌ Error calculating stats for '{col}': {e}")
            numeric_analysis[col] = {'error': str(e)}
    
    return numeric_analysis


def analyze_string_columns(df: pl.DataFrame, string_cols: List[str]) -> Dict:
    """Detailed analysis of string columns"""
    
    print(f"\n📝 STRING COLUMNS ANALYSIS ({len(string_cols)} columns)")
    print("─" * 80)
    
    string_analysis = {}
    
    for col in string_cols:
        try:
            non_null_count = df.shape[0] - df[col].null_count()
            unique_count = df[col].n_unique()
            
            col_analysis = {
                'count': non_null_count,
                'unique_count': unique_count,
                'uniqueness_ratio': (unique_count/non_null_count)*100 if non_null_count > 0 else 0
            }
            
            print(f"\n📄 {col.upper()}")
            print(f"   Count (non-null):    {non_null_count:,}")
            print(f"   Unique values:       {unique_count:,}")
            print(f"   Uniqueness ratio:    {col_analysis['uniqueness_ratio']:.1f}%")
            
            # Show most frequent values
            if unique_count > 0:
                top_values = (df[col]
                            .value_counts()
                            .head(5)
                            .sort('count', descending=True))
                
                print("   Top 5 values:")
                top_values_list = []
                for row in top_values.iter_rows():
                    value, count = row
                    pct = (count / non_null_count) * 100
                    print(f"     '{value}': {count:,} ({pct:.1f}%)")
                    top_values_list.append({'value': value, 'count': count, 'percentage': pct})
                
                col_analysis['top_values'] = top_values_list
            
            string_analysis[col] = col_analysis
                
        except Exception as e:
            print(f"   ❌ Error analyzing string column '{col}': {e}")
            string_analysis[col] = {'error': str(e)}
    
    return string_analysis


def get_column_insights(df: pl.DataFrame, column_name: str) -> Dict:
    """Get detailed insights for a specific column"""
    
    if column_name not in df.columns:
        return {'error': f'Column {column_name} not found'}
    
    col_data = df[column_name]
    insights = {
        'column_name': column_name,
        'dtype': str(col_data.dtype),
        'total_count': df.shape[0],
        'null_count': col_data.null_count(),
        'non_null_count': df.shape[0] - col_data.null_count(),
        'unique_count': col_data.n_unique()
    }
    
    # Add type-specific insights
    if col_data.dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
        insights.update({
            'min': col_data.min(),
            'max': col_data.max(),
            'mean': col_data.mean(),
            'median': col_data.median(),
            'std': col_data.std()
        })
    elif col_data.dtype == pl.Utf8:
        non_null_data = col_data.drop_nulls()
        if non_null_data.len() > 0:
            lengths = non_null_data.str.len_chars()
            insights.update({
                'min_length': lengths.min(),
                'max_length': lengths.max(),
                'avg_length': lengths.mean()
            })
    
    return insights


def generate_summary_statistics(df: pl.DataFrame) -> pl.DataFrame:
    """Generate comprehensive summary statistics"""
    
    # Use polars describe method for comprehensive stats
    summary = df.describe()
    return summary


def calculate_correlation_matrix(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate correlation matrix for numeric columns"""
    
    # Select only numeric columns
    numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]]
    
    if not numeric_cols:
        return None
    
    numeric_df = df.select(numeric_cols)
    
    # Calculate correlation matrix using polars
    # Note: Polars doesn't have built-in correlation, so we'll use a workaround
    correlation_data = {}
    
    for col1 in numeric_cols:
        correlations = []
        for col2 in numeric_cols:
            if col1 == col2:
                correlations.append(1.0)
            else:
                # Calculate Pearson correlation manually
                x = numeric_df[col1].drop_nulls()
                y = numeric_df[col2].drop_nulls()
                
                if len(x) > 1 and len(y) > 1:
                    # Simple correlation calculation
                    mean_x = x.mean()
                    mean_y = y.mean()
                    
                    numerator = ((x - mean_x) * (y - mean_y)).sum()
                    denominator = (((x - mean_x) ** 2).sum() * ((y - mean_y) ** 2).sum()) ** 0.5
                    
                    if denominator != 0:
                        corr = numerator / denominator
                    else:
                        corr = 0.0
                else:
                    corr = 0.0
                    
                correlations.append(corr)
        
        correlation_data[col1] = correlations
    
    return pl.DataFrame(correlation_data)


def analyze_missing_data(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze missing data patterns"""
    
    missing_data = []
    
    for col in df.columns:
        missing_count = df[col].null_count()
        missing_pct = (missing_count / df.shape[0]) * 100
        
        missing_data.append({
            'Column': col,
            'Missing_Count': missing_count,
            'Missing_Percentage': missing_pct,
            'Data_Type': str(df[col].dtype)
        })
    
    missing_df = pl.DataFrame(missing_data)
    return missing_df.sort('Missing_Count', descending=True)
