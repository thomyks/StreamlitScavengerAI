"""
Visualization Utilities Module
Helper functions for creating visualizations with Polars data
"""

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict


def polars_to_pandas_for_viz(df: pl.DataFrame) -> 'pandas.DataFrame':
    """Convert Polars DataFrame to Pandas for visualization compatibility"""
    return df.to_pandas()


def create_distribution_plot(df: pl.DataFrame, column: str, title: str = None):
    """Create distribution plot for a numeric column"""
    if title is None:
        title = f'Distribution of {column}'
    
    # Convert to pandas for plotly compatibility
    pandas_df = polars_to_pandas_for_viz(df.select([column]))
    
    fig = px.histogram(pandas_df, x=column, title=title)
    fig.update_layout(
        xaxis_title=column,
        yaxis_title='Frequency',
        showlegend=False
    )
    return fig


def create_scatter_plot(df: pl.DataFrame, x_col: str, y_col: str, title: str = None):
    """Create scatter plot for two numeric columns"""
    if title is None:
        title = f'{x_col} vs {y_col}'
    
    # Convert to pandas for plotly compatibility
    pandas_df = polars_to_pandas_for_viz(df.select([x_col, y_col]))
    
    fig = px.scatter(pandas_df, x=x_col, y=y_col, title=title)
    return fig


def create_missing_data_chart(df: pl.DataFrame):
    """Create bar chart showing missing data by column with improved label handling"""
    missing_data = []
    
    for col in df.columns:
        missing_count = df[col].null_count()
        missing_pct = (missing_count / df.shape[0]) * 100
        missing_data.append({
            'Column': col,
            'Missing_Count': missing_count,
            'Missing_Percentage': missing_pct
        })
    
    missing_df = pl.DataFrame(missing_data).sort('Missing_Percentage', descending=True)
    pandas_missing = polars_to_pandas_for_viz(missing_df)
    
    # Determine the best visualization approach based on number of columns
    num_columns = len(pandas_missing)
    
    if num_columns <= 15:
        # Standard vertical bar chart for small datasets
        fig = px.bar(
            pandas_missing, 
            x='Column', 
            y='Missing_Percentage',
            title='Missing Data by Column',
            labels={'Missing_Percentage': 'Missing %'}
        )
        fig.update_xaxes(
            tickangle=45,
            tickfont=dict(size=10)
        )
        fig.update_layout(
            height=500,
            margin=dict(b=120)  # More bottom margin for angled labels
        )
    
    elif num_columns <= 30:
        # Vertical labels for medium datasets
        fig = px.bar(
            pandas_missing, 
            x='Column', 
            y='Missing_Percentage',
            title='Missing Data by Column',
            labels={'Missing_Percentage': 'Missing %'}
        )
        fig.update_xaxes(
            tickangle=90,  # Vertical labels
            tickfont=dict(size=8),  # Smaller font
            title_font=dict(size=12)
        )
        fig.update_layout(
            height=600,  # Taller chart
            margin=dict(b=150)  # Much more bottom margin for vertical labels
        )
    
    else:
        # Horizontal bar chart for large datasets (best for many columns)
        fig = px.bar(
            pandas_missing, 
            x='Missing_Percentage', 
            y='Column',
            title='Missing Data by Column',
            labels={'Missing_Percentage': 'Missing %'},
            orientation='h'
        )
        fig.update_layout(
            height=max(400, num_columns * 15),  # Dynamic height based on number of columns
            yaxis=dict(
                tickfont=dict(size=8),
                automargin=True  # Auto adjust margins for y-axis labels
            ),
            margin=dict(l=120)  # More left margin for column names
        )
        # Reverse the order so highest missing data is at the top
        fig.update_yaxes(categoryorder='total ascending')
    
    return fig


def create_correlation_heatmap(correlation_matrix: pl.DataFrame):
    """Create correlation heatmap"""
    if correlation_matrix is None:
        return None
    
    pandas_corr = polars_to_pandas_for_viz(correlation_matrix)
    
    fig = px.imshow(
        pandas_corr.values,
        x=pandas_corr.columns,
        y=pandas_corr.columns,
        title='Correlation Matrix',
        color_continuous_scale='RdBu_r',
        aspect='auto'
    )
    
    return fig


def create_value_counts_charts(df: pl.DataFrame, column: str, top_n: int = 10):
    """Create bar and pie charts for value counts"""
    if df[column].n_unique() > 50:
        return None, None
    
    # Get value counts
    value_counts = df[column].value_counts().head(top_n).sort('count', descending=True)
    
    # Convert to pandas for visualization
    pandas_vc = polars_to_pandas_for_viz(value_counts)
    
    # Bar chart
    bar_fig = px.bar(
        pandas_vc, 
        x=column, 
        y='count', 
        title=f'Top {top_n} Values - {column}'
    )
    
    # Pie chart
    pie_fig = px.pie(
        pandas_vc, 
        names=column, 
        values='count', 
        title=f'Distribution - {column}'
    )
    
    return bar_fig, pie_fig


def create_groupby_chart(result_df: pl.DataFrame, x_col: str, y_col: str, chart_type: str = 'bar'):
    """Create chart for groupby analysis results"""
    pandas_result = polars_to_pandas_for_viz(result_df)
    
    if chart_type == 'bar':
        fig = px.bar(pandas_result, x=x_col, y=y_col)
    elif chart_type == 'line':
        fig = px.line(pandas_result, x=x_col, y=y_col)
    else:
        fig = px.bar(pandas_result, x=x_col, y=y_col)
    
    return fig


def create_data_quality_summary_chart(analysis_results: Dict):
    """Create summary chart of data quality metrics"""
    
    if 'summary' not in analysis_results:
        return None
    
    summary = analysis_results['summary']
    
    # Create data quality metrics
    metrics = {
        'Metric': ['Data Completeness', 'Numeric Columns', 'String Columns'],
        'Value': [
            summary['completeness_percentage'],
            summary['numeric_columns_count'],
            summary['string_columns_count']
        ],
        'Type': ['Percentage', 'Count', 'Count']
    }
    
    metrics_df = pl.DataFrame(metrics)
    pandas_metrics = polars_to_pandas_for_viz(metrics_df)
    
    fig = px.bar(
        pandas_metrics, 
        x='Metric', 
        y='Value', 
        color='Type',
        title='Data Quality Summary'
    )
    
    return fig
