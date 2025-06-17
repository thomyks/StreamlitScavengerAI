# Import necessary libraries
import streamlit as st
import json
import polars as pl
from data_loader import load_data_file
from data_analyzer import (
    analyze_dataset_structure_and_nulls, 
    generate_summary_statistics,
    analyze_missing_data,
    show_dataset_info,
    polars_to_pandas_for_viz,
    create_missing_data_chart
)
from schema_generator import (
    generate_schema_with_auto_batching,
    test_claude_connection,
    format_for_copy_paste,
    generate_enhanced_schema_with_descriptions,
    generate_enhanced_schema_with_auto_batching,
    extract_column_samples,
    call_claude_api_robust,
    generate_column_descriptions_with_business_context
)
from style_utils import apply_custom_styles

def get_current_claude_model():
    """Get the name of the current Claude model being used"""
    return "Claude Sonnet 4 (May 2025)" 


# Page Configuration
st.set_page_config(
    page_title="Scavenger AI",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styles from external CSS file
apply_custom_styles()

def main():
    # Initialize session state FIRST
    if 'data' not in st.session_state:
        st.session_state.data = None
    if 'filename' not in st.session_state:
        st.session_state.filename = None
    if 'all_dataframes' not in st.session_state:
        st.session_state.all_dataframes = {}
    if 'generated_descriptions' not in st.session_state:
        st.session_state.generated_descriptions = {}
    if 'generated_schemas' not in st.session_state:
        st.session_state.generated_schemas = {}
    
    # Get current model information
    model_name = get_current_claude_model()
    
    # Header
    st.markdown('<div class="main-header">From CSV to Schema in Seconds</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Automated Schema & Column Documentation Generator for MySQL</div>', unsafe_allow_html=True)
    
    # Model indicator
    st.markdown(
        f"""<div style='background-color: #3B4D61; padding: 8px 15px; border-radius: 5px; 
        display: inline-block; color: white; font-size: 0.8rem; margin-bottom: 20px;'>
        <b>Powered by:</b> {model_name}</div>""", 
        unsafe_allow_html=True
    )
    
    # Sidebar Navigation with Radio Buttons - Four Main Components
    st.sidebar.title("Navigation")
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "",
        ["📤 Data Loading", "📊 Descriptive Analysis", "📝 Description Generation", "🗄️ Schema Generation"],
        index=0
    )
    
    # Dataset Switcher in the sidebar
    st.sidebar.markdown("---")
    
    # Display dataset selector if datasets are available
    if st.session_state.all_dataframes:
        st.sidebar.markdown("### 📊 Dataset Selection")
        
        # Determine the current index for the selectbox
        current_index = 0
        if st.session_state.filename and st.session_state.filename in st.session_state.all_dataframes:
            current_index = list(st.session_state.all_dataframes.keys()).index(st.session_state.filename)
        
        selected_dataset = st.sidebar.selectbox(
            "Select active dataset:",
            list(st.session_state.all_dataframes.keys()),
            key="dataset_selector",
            index=current_index
        )
        
        # Update current dataset when selection changes
        if selected_dataset != st.session_state.filename:
            st.session_state.data = st.session_state.all_dataframes[selected_dataset]
            st.session_state.filename = selected_dataset
            st.sidebar.success(f"✅ Switched to {selected_dataset}")
    
    # Add some space and info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ About")
    st.sidebar.info(f"**Scavenger AI** uses Polars for fast data processing and **{model_name}** for intelligent schema generation.")

    # COMPONENT ONE: Data Loading
    if page == "📤 Data Loading":
        st.header("📤 Component One: Data Loading")
        st.markdown("**Goal:** Automatically detect and parse CSV structures with comprehensive error handling, supporting diverse formatting edge cases using Polars for superior performance.")
        
        # Show current active dataset if one exists
        if st.session_state.data is not None and st.session_state.filename is not None:
            show_dataset_info(st.session_state.data, st.session_state.filename, show_preview=True)
            
            if len(st.session_state.all_dataframes) > 1:
                st.info(f"📊 {len(st.session_state.all_dataframes)} datasets loaded. Use the sidebar to switch between them.")
                
            st.markdown("---")
            if st.button("Load More Data"):
                pass  # This is a trick to collapse the previous section when button is clicked
        
        # Create tabs for single and multiple file uploading
        upload_tab1, upload_tab2 = st.tabs(["Single File Upload", "Multiple Files Upload"])
        
        with upload_tab1:
            # Single file uploader
            uploaded_file = st.file_uploader(
                "Choose your data file",
                type=['csv'],
                help="Supported formats: CSV only (powered by Polars for fast processing)"
            )
            
            if uploaded_file is not None:
                try:
                    # Use our optimized Polars-based data loader
                    data = load_data_file(uploaded_file)
                    
                    if data is not None:
                        # Store in both the active dataset and the collection of all datasets
                        st.session_state.data = data
                        st.session_state.filename = uploaded_file.name
                        st.session_state.all_dataframes[uploaded_file.name] = data
                        
                        st.success(f"✅ Successfully loaded: {uploaded_file.name}")
                        
                        # Quick preview - convert to pandas for display
                        display_data = polars_to_pandas_for_viz(data)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Rows", f"{data.shape[0]:,}")
                        with col2:
                            st.metric("Columns", f"{data.shape[1]:,}")
                        with col3:
                            st.metric("Memory", f"{data.estimated_size('mb'):.1f} MB")
                        
                        st.subheader("📋 Data Preview")
                        st.dataframe(display_data.head(10), use_container_width=True)
                    else:
                        st.error("❌ Failed to load the file. Please check the format.")
                    
                except Exception as e:
                    st.error(f"❌ Error loading file: {str(e)}")
            
            else:
                st.info("👆 Please upload a file to get started")
                
        with upload_tab2:
            # Multiple file uploader
            uploaded_files = st.file_uploader(
                "Choose multiple data files",
                type=['csv'],
                accept_multiple_files=True,
                help="Supported formats: CSV only (powered by Polars for fast processing)"
            )
            
            if uploaded_files:
                # Initialize a dictionary to store all loaded dataframes
                all_dataframes = {}
                
                # Process each file
                for uploaded_file in uploaded_files:
                    try:
                        st.write(f"Processing: {uploaded_file.name}")
                        # Use our optimized Polars-based data loader
                        data = load_data_file(uploaded_file)
                        
                        if data is not None:
                            all_dataframes[uploaded_file.name] = data
                            st.success(f"✅ Successfully loaded: {uploaded_file.name} ({data.shape[0]} rows × {data.shape[1]} columns)")
                        else:
                            st.error(f"❌ Failed to load {uploaded_file.name}. Please check the format.")
                        
                    except Exception as e:
                        st.error(f"❌ Error loading {uploaded_file.name}: {str(e)}")
                
                # If files were loaded successfully
                if all_dataframes:
                    # Store in session state
                    st.session_state.all_dataframes = all_dataframes
                    
                    # Add a selectbox to choose which dataframe to work with
                    selected_df = st.selectbox(
                        "Select a dataset to work with:",
                        list(all_dataframes.keys())
                    )
                    
                    # Set the selected dataframe as the current one
                    st.session_state.data = all_dataframes[selected_df]
                    st.session_state.filename = selected_df
                    
                    # Show preview of selected dataframe
                    st.subheader(f"📋 Preview of {selected_df}")
                    display_data = polars_to_pandas_for_viz(st.session_state.data)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Rows", f"{st.session_state.data.shape[0]:,}")
                    with col2:
                        st.metric("Columns", f"{st.session_state.data.shape[1]:,}")
                    with col3:
                        st.metric("Memory", f"{st.session_state.data.estimated_size('mb'):.1f} MB")
                    
                    st.dataframe(display_data.head(10), use_container_width=True)
            
            else:
                st.info("👆 Please upload one or more files to get started")

    # COMPONENT TWO: Descriptive Analysis
    elif page == "📊 Descriptive Analysis":
        st.header("📊 Component Two: Descriptive Analysis")
        st.markdown("**Goal:** Analyze the data and compute basic descriptive statistics with a focus on missing value treatment and data quality assessment.")
        
        # Show dataset info and exit early if no data
        if not show_dataset_info(st.session_state.data, st.session_state.filename):
            return
        
        data = st.session_state.data
        
        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["📊 Summary", "🔍 Data Types", "❌ Missing Data"])
        
        with tab1:
            st.subheader("Statistical Summary")
            if st.button("🔄 Generate Summary"):
                # Call our enhanced summary function
                summary_stats = generate_summary_statistics(data)
                summary_pandas = polars_to_pandas_for_viz(summary_stats)
                st.dataframe(summary_pandas, use_container_width=True)
        
        with tab2:
            st.subheader("Data Types Analysis")
            st.info("💡 **Data types shown below are automatically inferred by Polars during CSV parsing**")
            if st.button("🔍 Analyze Data Types"):
                # Use our enhanced data analyzer
                analysis_results = analyze_dataset_structure_and_nulls(data, st.session_state.filename or "Dataset")
                
                # Create a data types summary using Polars
                dtype_data = []
                for col, info in analysis_results['columns'].items():
                    dtype_data.append({
                        'Column': col,
                        'Data Type': info.get('dtype', 'Unknown'),
                        'Non-Null Count': info.get('non_null_count', 0),
                        'Null Count': info.get('null_count', 0),
                        'Null %': round(info.get('null_percentage', 0), 2)
                    })
                
                dtype_df = pl.DataFrame(dtype_data)
                # Convert to pandas only for display
                dtype_pandas = polars_to_pandas_for_viz(dtype_df)
                st.dataframe(dtype_pandas, use_container_width=True)
        
        with tab3:
            st.subheader("Missing Data Analysis")
            if st.button("🔍 Analyze Missing Data"):
                # Call our enhanced missing data analysis function
                missing_analysis = analyze_missing_data(data)
                missing_pandas = polars_to_pandas_for_viz(missing_analysis)
                
                # Add visualization options for better handling of many columns
                num_columns = len(data.columns)
                if num_columns > 20:
                    st.info(f"📊 Detected {num_columns} columns. The chart will automatically use horizontal layout for better readability.")
                
                # Visualization with improved handling for many columns
                fig = create_missing_data_chart(data)
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(missing_pandas, use_container_width=True)
        
    # COMPONENT THREE: Description Generation
    elif page == "📝 Description Generation":
        st.header("📝 Component Three: Description Generation")
        st.markdown("**Goal:** Generate intelligent business descriptions for columns using AI analysis of data patterns, column names, and sample values.")
        
        # Show dataset info and exit early if no data
        if not show_dataset_info(st.session_state.data, st.session_state.filename):
            return
        
        data = st.session_state.data
        
        st.markdown("---")
        
        # Description generation options
        st.subheader("🔍 Generate Column Descriptions")
        
        # Configuration options
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**🧠 Business Context for AI**")
            # Auto-infer table name from filename
            default_table_name = st.session_state.filename.replace('.csv', '').replace('.CSV', '') if st.session_state.filename else "my_table"
            table_name = st.text_input(
                "Table/System Name", 
                value=default_table_name, 
                help="🧠 Auto-inferred from filename. This helps Claude AI understand your business domain for better descriptions",
                key="desc_table_name"
            )
            st.caption("💡 Auto-filled from CSV filename - edit if needed")
            
        with col2:
            st.markdown("**📊 Sample Analysis Size**")
            sample_size = st.slider(
                "Sample Values per Column",
                min_value=3,
                max_value=10,
                value=5,
                help="How many sample values Claude analyzes per column. More samples = better context but higher API costs."
            )
            st.caption("💰 Higher values = more accurate but costlier")
        
        # Buttons side by side
        col1, col2 = st.columns(2)
        with col1:
            test_connection = st.button("🔗 Test Claude API Connection", help="Verify that Claude AI is accessible")
        with col2:
            generate_descriptions = st.button("🤖 Generate Column Descriptions", type="primary", help="Use Claude AI to generate intelligent column descriptions")
        
        # Handle test connection
        if test_connection:
            model_name = get_current_claude_model()
            with st.spinner(f"Testing {model_name} API connection..."):
                connection_ok = test_claude_connection()
                if connection_ok:
                    st.success(f"✅ {model_name} connection successful!")
                else:
                    st.error(f"❌ {model_name} connection failed. Please check your API key.")
        
        st.markdown("---")
        
        # Generate descriptions
        if generate_descriptions:
            if not table_name.strip():
                st.error("Please provide a table name.")
                return
                
            with st.spinner("🔍 Analyzing column patterns and generating descriptions..."):
                # Use centralized business logic from schema_generator.py
                descriptions, error = generate_column_descriptions_with_business_context(
                    data, table_name, sample_size
                )
                
                if descriptions and not error:
                    # Store descriptions in session state for use in schema generation
                    st.session_state.generated_descriptions[st.session_state.filename] = descriptions
                    
                    st.success(f"✅ Generated descriptions for {len(descriptions)} columns!")
                    
                    # Display results in tabs
                    tab1, tab2, tab3 = st.tabs(["📝 Descriptions", "📋 Table Format", "📄 Copy-Paste"])
                    
                    with tab1:
                        st.subheader("Generated Column Descriptions")
                        for col, desc in descriptions.items():
                            st.write(f"**{col}:** {desc}")
                    
                    with tab2:
                        st.subheader("Table Format")
                        desc_data = [
                            {"Column": col, "Description": desc} 
                            for col, desc in descriptions.items()
                        ]
                        desc_df = pl.DataFrame(desc_data)
                        desc_pandas = polars_to_pandas_for_viz(desc_df)
                        st.dataframe(desc_pandas, use_container_width=True)
                    
                    with tab3:
                        st.subheader("Copy-Paste Format")
                        formatted_desc = json.dumps(descriptions, indent=2)
                        st.code(formatted_desc, language='json')
                        
                        # Download option
                        st.download_button(
                            label="📥 Download Descriptions (JSON)",
                            data=formatted_desc,
                            file_name=f"{table_name}_descriptions.json",
                            mime="application/json"
                        )
                else:
                    st.error(f"❌ {error}")

    # COMPONENT FOUR: Schema Generation  
    elif page == "🗄️ Schema Generation":
        st.header("🗄️ Component Four: Schema Generation")
        st.markdown("**Goal:** Generate production-ready SQL schemas using Claude AI for intelligent type inference and business descriptions.")
        
        # Show dataset info and exit early if no data
        if not show_dataset_info(st.session_state.data, st.session_state.filename):
            return
        
        data = st.session_state.data
        
        st.markdown("---")
        
        # Schema generation options
        st.subheader("🔧 Schema Generation Settings")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**🧠 Business Context for AI**")
            # Auto-infer table name from filename
            default_table_name = st.session_state.filename.replace('.csv', '').replace('.CSV', '') if st.session_state.filename else "my_table"
            table_name = st.text_input(
                "Table/System Name", 
                value=default_table_name, 
                help="🧠 Auto-inferred from filename. This helps Claude AI understand your business domain for better SQL type inference",
                key="schema_table_name"
            )
            st.caption("💡 Auto-filled from CSV filename - edit if needed")
        with col2:
            sample_size = st.slider("Sample Size", min_value=3, max_value=10, value=5, 
                                   help="Number of sample values to analyze per column")
        with col3:
            total_columns = len(data.columns)
            if total_columns > 20:
                st.info(f"📦 Large dataset detected: {total_columns} columns will be processed in batches of 20")
            else:
                st.info(f"📊 Small dataset: {total_columns} columns will be processed at once")
        
        # Test Claude connection
        if st.button("🔗 Test Claude AI Connection"):
            with st.spinner("Testing Claude API connection..."):
                connection_ok = test_claude_connection()
                if connection_ok:
                    st.success("✅ Claude AI connection successful!")
                else:
                    st.error("❌ Claude AI connection failed. Please check your API key.")
        
        st.markdown("---")
        
        # Check if we have pre-generated descriptions for this dataset
        has_descriptions = st.session_state.filename in st.session_state.generated_descriptions
        if has_descriptions:
            descriptions_count = len(st.session_state.generated_descriptions[st.session_state.filename])
            st.info(f"💡 Found {descriptions_count} AI-generated descriptions for this dataset. These will enhance schema generation!")
            
            # Option to use enhanced schema generation
            use_enhanced = st.checkbox("🚀 Use Enhanced Schema Generation (with AI descriptions)", value=True, 
                                     help="Use pre-generated descriptions to make better type inference decisions")
        else:
            st.warning("⚠️ No AI descriptions found for this dataset. Consider generating descriptions first for better results.")
            use_enhanced = False
        
        st.markdown("---")
        
        # Generate schema
        if st.button("🤖 Generate SQL Schema", type="primary", help="Use Claude AI to generate optimized SQL schema"):
            if not table_name.strip():
                st.error("Please provide a table name.")
                return
                
            with st.spinner("🔍 Analyzing data structure and generating SQL schema..."):
                try:
                    # Check if we should use enhanced generation
                    if use_enhanced and has_descriptions:
                        st.info("🚀 Using enhanced schema generation with AI descriptions...")
                        descriptions = st.session_state.generated_descriptions[st.session_state.filename]
                        
                        # Use batch-enabled enhanced schema generation for large datasets
                        total_columns = len(data.columns)
                        if total_columns > 20:
                            st.info(f"📊 Processing {total_columns} columns in batches...")
                            ddl, enhanced_descriptions = generate_enhanced_schema_with_auto_batching(data, table_name, descriptions, sample_size)
                        else:
                            ddl, enhanced_descriptions = generate_enhanced_schema_with_descriptions(data, table_name, descriptions, sample_size)
                            
                        descriptions = enhanced_descriptions  # Use the enhanced descriptions for display
                    else:
                        st.info("📊 Using standard schema generation...")
                        # Use the auto-batching schema generator from schema_generator.py
                        ddl, descriptions = generate_schema_with_auto_batching(data, table_name)
                    
                    if ddl and descriptions and ddl != "-- Error: Failed to generate schema after multiple attempts":
                        st.success(f"✅ Schema generated successfully for table '{table_name}'!")
                        
                        # Display results in tabs
                        tab1, tab2, tab3, tab4 = st.tabs([
                            "🗄️ SQL Schema", 
                            "📝 Column Descriptions", 
                            "📋 Copy-Paste Formats",
                            "📖 Implementation Guide"
                        ])
                        
                        with tab1:
                            st.subheader("Generated SQL DDL")
                            st.code(ddl, language='sql')
                            
                            # Download DDL
                            st.download_button(
                                label="📥 Download SQL Schema",
                                data=ddl,
                                file_name=f"{table_name}_schema.sql",
                                mime="text/sql"
                            )
                        
                        with tab2:
                            st.subheader("Column Descriptions")
                            st.code(descriptions, language='json')
                            
                            # Parse and display as table
                            try:
                                desc_data = json.loads(descriptions)
                                desc_df_data = [
                                    {"Column": col, "Description": desc} 
                                    for col, desc in desc_data.items()
                                ]
                                desc_df = pl.DataFrame(desc_df_data)
                                desc_pandas = polars_to_pandas_for_viz(desc_df)
                                st.dataframe(desc_pandas, use_container_width=True)
                                
                                # Download descriptions
                                st.download_button(
                                    label="📥 Download Descriptions (JSON)",
                                    data=descriptions,
                                    file_name=f"{table_name}_descriptions.json",
                                    mime="application/json"
                                )
                            except:
                                st.info("Descriptions shown in raw format above")
                        
                        with tab3:
                            st.subheader("Copy-Paste Ready Formats")
                            
                            # Generate different formats using your schema_generator functions
                            complete_format = format_for_copy_paste(ddl, descriptions, table_name)
                            
                            format_type = st.selectbox(
                                "Choose Format:",
                                ["Complete Package", "DDL Only", "Descriptions Only"]
                            )
                            
                            if format_type == "Complete Package":
                                st.code(complete_format, language='sql')
                                st.download_button(
                                    label="📥 Download Complete Package",
                                    data=complete_format,
                                    file_name=f"{table_name}_complete_schema.sql",
                                    mime="text/sql"
                                )
                            elif format_type == "DDL Only":
                                st.code(ddl, language='sql')
                            else:
                                st.code(descriptions, language='json')
                        
                        with tab4:
                            st.subheader("Implementation Guide")
                            st.markdown(f"""
                            ### 🚀 How to Implement Your Schema
                            
                            1. **Create the Table**
                               ```sql
                               -- Copy the DDL from the SQL Schema tab
                               {ddl.split(';')[0][:100] if ddl else ''}...
                               ```
                            
                            2. **Add Indexes** (recommended)
                               ```sql
                               -- Add indexes for better performance
                               CREATE INDEX idx_{table_name}_id ON {table_name} (id);
                               ```
                            
                            3. **Insert Sample Data**
                               ```sql
                               -- Test your schema with sample data
                               INSERT INTO {table_name} (...) VALUES (...);
                               ```
                            
                            4. **Validate Data Quality**
                               ```sql
                               -- Check for data issues
                               SELECT COUNT(*) FROM {table_name};
                               SELECT * FROM {table_name} LIMIT 10;
                               ```
                            """)
                            
                            # Schema statistics
                            try:
                                desc_data = json.loads(descriptions)
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Generated Columns", len(desc_data))
                                with col2:
                                    st.metric("Source Rows", f"{data.shape[0]:,}")
                                with col3:
                                    st.metric("Est. Memory", f"{data.estimated_size('mb'):.1f} MB")
                            except:
                                pass
                    else:
                        st.error("Failed to generate schema. Please try again.")
                        if ddl.startswith("-- Error:"):
                            st.error(ddl)
                        
                except Exception as e:
                    st.error(f"Error generating schema: {str(e)}")
                    import traceback
                    with st.expander("Debug Information"):
                        st.code(traceback.format_exc())
    # Footer
    st.markdown("---")

if __name__ == "__main__":
    main()
