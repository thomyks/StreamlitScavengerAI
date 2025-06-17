# Streamlit Scavenger AI 

**Production-Ready Data Analysis & Schema Generation Tool**

A powerful AI-driven application that automatically analyzes datasets and generates intelligent database schemas using Claude AI. Built with Streamlit for intuitive data exploration and visualization.

## 🚀 Features

- **📊 Smart Data Analysis**: Automated dataset structure analysis with missing data detection
- **🤖 AI-Powered Schema Generation**: Generate optimized database schemas using Claude Sonnet 4
- **📈 Interactive Visualizations**: Beautiful charts and graphs using Plotly
- **⚡ High-Performance Processing**: Lightning-fast data processing with Polars
- **🎨 Modern UI**: Clean, responsive interface with custom styling
- **🔒 Secure Configuration**: Environment-based API key management
- **📝 Detailed Documentation**: Comprehensive schema documentation with business context

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| **Frontend** | Streamlit | 1.45.1 |
| **Data Processing** | Polars | 1.30.0 |
| **Visualization** | Plotly | 6.1.2 |
| **LLM Integration** | Claude API | Sonnet 4 |
| **HTTP Requests (Claude)** | Requests | 2.32.4 |
| **Environment Management** | python-dotenv | 1.0.0 |

## 📦 Installation

### Prerequisites
- Python 3.8+
- Claude API key (from Anthropic)

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/StreamlitScavengerAI.git
   cd StreamlitScavengerAI
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env and add your Claude API key
   echo "CLAUDE_API_KEY=your_api_key_here" > .env
   ```

5. **Launch the application**
   ```bash
   streamlit run streamlit_app.py
   ```

6. **Open in browser**
   - Navigate to `http://localhost:8501`

## 🏗️ Project Structure

```
StreamlitScavengerAI/
├── streamlit_app.py          # Main Streamlit application
├── data_loader.py            # Data loading utilities
├── data_analyzer.py          # Core data analysis functions
├── schema_generator.py       # AI-powered schema generation
├── style_utils.py           # UI styling utilities
├── styles.css               # Custom CSS styles
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not tracked)
├── .gitignore              # Git ignore rules
├── README.md               # This file
```

## 🎯 Usage

### 1. Data Upload
- Support for CSV
- Automatic data type detection
- Real-time file validation

### 2. Data Analysis
- **Structure Analysis**: Column types, null values, data quality metrics
- **Statistical Summary**: Descriptive statistics for numerical columns
- **Missing Data Patterns**: Visual analysis of data completeness

### 3. Schema Generation
- **Automated Schema Creation**: AI-generated database schemas
- **Business Context Integration**: Intelligent column descriptions
- **Multiple Output Formats**: SQL DDL, JSON schema, documentation

### 4. Visualization
- Interactive charts for data distribution
- Missing data visualisation
- Statistical summaries with visual components

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Required
CLAUDE_API_KEY=your_claude_api_key_here

# Optional
STREAMLIT_THEME=light
DEBUG_MODE=false
MAX_FILE_SIZE_MB=100
```

### Streamlit Configuration

The app uses custom styling through `styles.css`. Modify this file to customize the appearance.

## 📊 API Integration

### Claude AI Integration
- **Model**: Claude Sonnet 4 (May 2025)
- **Rate Limiting**: Built-in retry logic with exponential backoff
- **Error Handling**: Comprehensive error management and fallbacks
- **Security**: API keys managed through environment variables

## 🚦 Production Deployment

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0"]
```

## 🧪 Testing

```bash
# Run data analysis tests
python -m pytest tests/test_data_analyzer.py

# Test Claude API connection
python -c "from schema_generator import test_claude_connection; test_claude_connection()"

# Validate data loading
python -c "from data_loader import load_data_file; print('Data loader working!')"
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.45.1-red.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
