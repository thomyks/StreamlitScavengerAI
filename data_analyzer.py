import polars as pl
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional, Any, Union, Tuple

# Constants
HIGH_NULL_THRESHOLD = 50.0
OUTLIER_IQR_MULTIPLIER = 1.5
TOP_VALUES_LIMIT = 5


def analyze_dataset_structure_and_nulls(df: pl.DataFrame, name: str) -> Dict[str, Any]:
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


def analyze_numeric_columns(df: pl.DataFrame, numeric_cols: List[str]) -> Dict[str, Any]:
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


def analyze_string_columns(df: pl.DataFrame, string_cols: List[str]) -> Dict[str, Dict[str, Any]]:
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


def generate_summary_statistics(df: pl.DataFrame) -> pl.DataFrame:
    """Generate comprehensive summary statistics"""
    
    # Use polars describe method for comprehensive stats
    summary = df.describe()
    return summary


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

# =============================================================================
# UI and Visualization Utility Functions
# =============================================================================

def polars_to_pandas_for_viz(df: pl.DataFrame):
    """Convert Polars DataFrame to Pandas for visualization compatibility"""
    return df.to_pandas()


def show_dataset_info(data, filename, show_preview=False):
    """Display dataset information in Streamlit UI"""
    if data is None or filename is None:
        st.warning("⚠️ No dataset loaded. Please upload a file first.")
        return False
    
    # Dataset info header
    st.markdown(f"### 📊 Current Dataset: `{filename}`")
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📝 Rows", f"{data.shape[0]:,}")
    with col2:
        st.metric("📊 Columns", f"{data.shape[1]:,}")
    with col3:
        st.metric("💾 Memory", f"{data.estimated_size('mb'):.1f} MB")
    
    # Optional preview
    if show_preview:
        st.markdown("#### 👁️ Data Preview")
        display_data = polars_to_pandas_for_viz(data)
        st.dataframe(display_data.head(5), use_container_width=True)
    
    return True


def create_missing_data_chart(df: pl.DataFrame):
    """Create a polished, interactive missing data visualization"""
    # Calculate missing data percentages with additional metrics
    missing_data = []
    total_rows = df.shape[0]
    
    for col in df.columns:
        missing_count = df[col].null_count()
        missing_pct = (missing_count / total_rows) * 100
        complete_count = total_rows - missing_count
        
        # Categorize data quality
        if missing_pct == 0:
            quality = "Complete"
        elif missing_pct < 10:
            quality = "Excellent"
        elif missing_pct < 25:
            quality = "Good"
        elif missing_pct < 50:
            quality = "Fair"
        elif missing_pct < 90:
            quality = "Poor"
        else:
            quality = "Critical"
        
        missing_data.append({
            'Column': col,
            'Missing_Percentage': missing_pct,
            'Missing_Count': missing_count,
            'Complete_Count': complete_count,
            'Quality': quality
        })
    
    # Convert to pandas for plotting
    missing_df = pl.DataFrame(missing_data).to_pandas()
    missing_df = missing_df.sort_values('Missing_Percentage', ascending=True)  # Ascending for better visual flow
    
    # Determine layout and styling based on number of columns
    num_columns = len(df.columns)
    
    # Create custom color scale based on data quality
    color_scale = [
        [0.0, "#2E8B57"],    # Complete - Sea green
        [0.1, "#32CD32"],    # Excellent - Lime green  
        [0.25, "#FFD700"],   # Good - Gold
        [0.5, "#FF8C00"],    # Fair - Dark orange
        [0.75, "#FF4500"],   # Poor - Orange red
        [1.0, "#DC143C"]     # Critical - Crimson
    ]
    
    if num_columns > 20:
        # Enhanced horizontal bar chart for many columns
        fig = px.bar(
            missing_df,
            x='Missing_Percentage',
            y='Column',
            orientation='h',
            color='Missing_Percentage',
            color_continuous_scale=color_scale,
            hover_data={
                'Missing_Count': True,
                'Complete_Count': True,
                'Quality': True,
                'Missing_Percentage': ':.1f'
            },
            height=max(600, num_columns * 15),
            title=f'📊 Data Completeness Analysis - {num_columns} columns × {total_rows:,} rows'
        )
        
        # Enhanced layout for horizontal chart
        fig.update_layout(
            yaxis={
                'categoryorder': 'total ascending',
                'title': {'text': 'Columns', 'font': {'size': 14}},
                'tickfont': {'size': 10}
            },
            xaxis={
                'title': {'text': 'Missing Data (%)', 'font': {'size': 14}},
                'tickfont': {'size': 12},
                'gridcolor': 'rgba(128,128,128,0.2)'
            },
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font={'family': 'Arial, sans-serif'},
            margin={'l': 120, 'r': 80, 't': 100, 'b': 80}
        )
        
        # Add reference lines
        fig.add_vline(x=10, line_width=1, line_dash="dash", line_color="green", 
                     annotation_text="Good (10%)", annotation_position="top")
        fig.add_vline(x=25, line_width=1, line_dash="dash", line_color="orange", 
                     annotation_text="Fair (25%)", annotation_position="top")
        fig.add_vline(x=50, line_width=1, line_dash="dash", line_color="red", 
                     annotation_text="Poor (50%)", annotation_position="top")
        
    else:
        # Enhanced vertical bar chart for fewer columns
        fig = px.bar(
            missing_df,
            x='Column',
            y='Missing_Percentage',
            color='Missing_Percentage',
            color_continuous_scale=color_scale,
            hover_data={
                'Missing_Count': True,
                'Complete_Count': True,
                'Quality': True,
                'Missing_Percentage': ':.1f'
            },
            height=500,
            title=f'📊 Data Completeness Analysis - {num_columns} columns × {total_rows:,} rows'
        )
        
        # Enhanced layout for vertical chart
        fig.update_layout(
            xaxis={
                'title': {'text': 'Columns', 'font': {'size': 14}},
                'tickangle': -45,
                'tickfont': {'size': 10}
            },
            yaxis={
                'title': {'text': 'Missing Data (%)', 'font': {'size': 14}},
                'tickfont': {'size': 12},
                'gridcolor': 'rgba(128,128,128,0.2)'
            },
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font={'family': 'Arial, sans-serif'},
            margin={'l': 80, 'r': 80, 't': 100, 'b': 120}
        )
        
        # Add reference lines
        fig.add_hline(y=10, line_width=1, line_dash="dash", line_color="green", 
                     annotation_text="Good (10%)", annotation_position="right")
        fig.add_hline(y=25, line_width=1, line_dash="dash", line_color="orange", 
                     annotation_text="Fair (25%)", annotation_position="right")
        fig.add_hline(y=50, line_width=1, line_dash="dash", line_color="red", 
                     annotation_text="Poor (50%)", annotation_position="right")
    
    # Enhance hover template
    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Missing: %{y:.1f}% (%{customdata[1]:,} rows)<br>"
        "Complete: %{customdata[2]:,} rows<br>"
        "Quality: %{customdata[3]}<br>"
        "<extra></extra>"
    )
    
    # Update hover info with custom data
    fig.update_traces(
        customdata=missing_df[['Column', 'Missing_Count', 'Complete_Count', 'Quality']].values,
        hovertemplate=hover_template if num_columns <= 20 else (
            "<b>%{y}</b><br>"
            "Missing: %{x:.1f}% (%{customdata[1]:,} rows)<br>"
            "Complete: %{customdata[2]:,} rows<br>"
            "Quality: %{customdata[3]}<br>"
            "<extra></extra>"
        )
    )
    
    return fig
