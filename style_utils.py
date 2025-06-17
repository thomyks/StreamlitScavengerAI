import streamlit as st
from pathlib import Path


def load_css_file(css_file_path: str) -> str:
    """Load CSS file and return its content as a string"""
    try:
        css_path = Path(css_file_path)
        if css_path.exists():
            with open(css_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            st.warning(f"CSS file not found: {css_file_path}")
            return ""
    except Exception as e:
        st.error(f"Error loading CSS file: {e}")
        return ""


def apply_custom_styles():
    """Apply custom CSS styles from external file"""
    # Get the directory of the current script
    current_dir = Path(__file__).parent
    css_file = current_dir / "styles.css"
    
    # Load CSS content
    css_content = load_css_file(str(css_file))
    
    if css_content:
        # Apply CSS to Streamlit
        st.markdown(f"""
        <style>
        {css_content}
        </style>
        """, unsafe_allow_html=True)
    else:
        # Fallback: basic styling if CSS file fails to load
        st.markdown("""
        <style>
        .main-header {
            font-size: 3rem;
            font-weight: bold;
            color: #FF4B4B;
            text-align: center;
            margin-bottom: 1rem;
        }
        .sub-header {
            font-size: 1.2rem;
            color: #666;
            text-align: center;
            margin-bottom: 2rem;
        }
        </style>
        """, unsafe_allow_html=True)
