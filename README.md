# Health Data Science-as-a-Service (HDSaaS)

A comprehensive Streamlit application for research groups working with large-scale NHS health datasets.

## Overview

This platform provides end-to-end data science solutions for health research, including:

- **Phenotype Development** - Codelist-driven clinical definitions using SNOMED CT, ICD-10, OPCS-4, Read v2, and BNF
- **Data Curation** - ETL, harmonisation, and derivations for GDPPR, HES, and disease registries
- **Data Quality Assurance** - Automated QA pipelines with comprehensive reporting
- **Data Linkage** - Cross-source patient matching and timeline harmonisation
- **RAG-Powered Q&A** - AI assistant for querying documentation (Pinecone + Claude)

## Quick Start

### Installation

```bash
# Clone or download the project
cd hdss_app

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Application

```bash
streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`

## Project Structure

```
hdss_app/
├── app.py              # Main Streamlit application
├── styles.css          # Custom CSS styling
├── requirements.txt    # Python dependencies
├── README.md           # This file
└── rag/                # RAG integration (optional)
    ├── embeddings.py   # Embedding generation
    ├── retrieval.py    # Pinecone retrieval
    └── generation.py   # Claude generation
```

## Features

### 1. Phenotype Development

- JSON-based phenotype specifications
- Multi-source compatibility (GDPPR, HES, registries)
- Version-controlled definitions
- PySpark implementation examples

### 2. Partial Curation Service

- Long-format primary care data (GDPPR)
- Secondary care transformations (HES APC, OP, A&E)
- Disease registry ingestion
- Schema documentation and data dictionaries

### 3. Full Curation Service

Everything in partial curation plus:

- Covariate derivation (comorbidities, medications, labs)
- Outcome derivation (MACE, mortality, hospitalisation)
- Time-varying features with configurable look-back windows
- Cohort generation with inclusion/exclusion criteria

### 4. Data Quality & Assurance

- Completeness, validity, consistency checks
- Distribution analysis and outlier detection
- Date plausibility validation
- Automated PDF/HTML/Markdown reports

### 5. RAG-Powered Q&A

Ask questions about:
- Phenotype definitions
- Curation rules and processes
- Technical capabilities
- Data quality procedures

## Enabling Production RAG

To enable the full RAG functionality with Pinecone and Claude:

1. Uncomment the optional dependencies in `requirements.txt`
2. Install the additional packages:
   ```bash
   pip install pinecone-client anthropic sentence-transformers
   ```
3. Set environment variables:
   ```bash
   export PINECONE_API_KEY=your_pinecone_key
   export PINECONE_INDEX=hdss-knowledge
   export ANTHROPIC_API_KEY=your_anthropic_key
   ```
4. Populate the Pinecone index with your documentation

## Customisation

### Styling

The application uses a clinical-modern aesthetic defined in `styles.css`. Key design tokens:

- **Colors**: Blue-teal gradient palette
- **Fonts**: IBM Plex Sans (body), Fraunces (headings), IBM Plex Mono (code)
- **Components**: Cards, tabs, pipelines, quality metrics

### Adding Content

- **Phenotypes**: Add new phenotype examples in the Phenotype Development tab
- **Use Cases**: Extend the use_cases list in app.py
- **FAQ**: Add questions to the faqs list in app.py

## Technical Stack

- **Frontend**: Streamlit
- **Styling**: Custom CSS
- **Data Processing**: PySpark (examples), Pandas (demo)
- **RAG**: Pinecone (vector storage), Claude (generation)

## Target Environments

This application is designed to work alongside NHS data in secure research environments:

- NHS England TRE
- SAIL Databank
- OpenSAFELY
- Institutional TREs

## License

© 2025 Health Data Science-as-a-Service

## Contact

For enquiries about services, please get in touch.
