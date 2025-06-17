import streamlit as st
from visualization_utils import polars_to_pandas_for_viz

def show_dataset_info(data, filename, show_preview=False):
    """
    Display information about the currently active dataset
    
    Args:
        data: Polars DataFrame containing the dataset
        filename: Name of the file
        show_preview: Whether to show a data preview (default: False)
    """
    if data is None:
        st.warning("⚠️ No dataset selected!")
        return False
    
    st.success(f"✅ Active dataset: {filename}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", f"{data.shape[0]:,}")
    with col2:
        st.metric("Columns", f"{data.shape[1]:,}")
    with col3:
        st.metric("Memory", f"{data.estimated_size('mb'):.1f} MB")
    
    if show_preview:
        with st.expander("📋 Data Preview", expanded=False):
            display_data = polars_to_pandas_for_viz(data)
            st.dataframe(display_data.head(10), use_container_width=True)
    
    return True
