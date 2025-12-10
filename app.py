"""
BHF Data Science Centre - Health Data Service
A comprehensive platform for research groups working with large-scale health datasets.
"""

import streamlit as st
from pathlib import Path
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="BHF Data Science Centre - Health Data Service",
    page_icon="❤️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Load custom CSS
def load_css():
    css_path = Path(__file__).parent / "styles.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

# ============================================================================
# HERO SECTION
# ============================================================================

st.markdown("""
<div class="hero-section">
    <div class="hero-content">
        <div class="hero-badge">BHF Data Science Centre</div>
        <h1 class="hero-title">Health Data Service</h1>
        <p class="hero-subtitle">Transforming raw health records into research-ready datasets.<br>
        Built for academic and clinical research groups working with NHS data at scale.</p>
        <div class="hero-stats">
            <div class="stat-item">
                <div class="stat-number">54M+</div>
                <div class="stat-label">Patient Records</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">8+</div>
                <div class="stat-label">Years Experience</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">100%</div>
                <div class="stat-label">Reproducible</div>
            </div>
        </div>
    </div>
    <div class="hero-visual">
        <div class="data-flow-diagram">
            <div class="flow-node node-1">Raw Data</div>
            <div class="flow-arrow">→</div>
            <div class="flow-node node-2">Curation</div>
            <div class="flow-arrow">→</div>
            <div class="flow-node node-3">Phenotypes</div>
            <div class="flow-arrow">→</div>
            <div class="flow-node node-4">Research</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Quick callouts
col1, col2, col3, col4, col5 = st.columns(5)

callouts = [
    ("🧬", "Phenotype Development", "Codelist-driven definitions"),
    ("🏥", "Primary/Secondary Care", "GDPPR, HES, registries"),
    ("📊", "Data Quality", "Automated QA pipelines"),
    ("🔄", "Reproducible", "Version-controlled workflows"),
    ("🔗", "Data Linkage", "Cross-source harmonisation")
]

for col, (icon, title, desc) in zip([col1, col2, col3, col4, col5], callouts):
    with col:
        st.markdown(f"""
        <div class="callout-card">
            <div class="callout-icon">{icon}</div>
            <div class="callout-title">{title}</div>
            <div class="callout-desc">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# SERVICES OVERVIEW
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>Services</h2>
    <p>End-to-end data science solutions for health research</p>
</div>
""", unsafe_allow_html=True)

# Service tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🧬 Phenotype Development",
    "📦 Partial Curation", 
    "🎯 Full Curation",
    "✅ Data Quality",
    "⚙️ Technical Stack",
    "🔗 Data Linkage"
])

# ----------------------------------------------------------------------------
# TAB 1: Phenotype Development
# ----------------------------------------------------------------------------
with tab1:
    st.markdown("""
    <div class="service-intro">
        <h3>Phenotype Development</h3>
        <p>Rigorous, reproducible clinical phenotype definitions using standardised clinical coding systems.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        st.markdown("""
        #### What We Provide
        
        - **Logic Specifications** — Structured JSON, DAG, or human-readable format
        - **Codelist Integration** — SNOMED CT, ICD-10, OPCS-4, Read v2, BNF
        - **Version Control** — Git-tracked phenotype definitions with full audit trail
        - **Multi-source Compatibility** — Works across GDPPR, HES, and disease registries
        
        #### Supported Registries
        
        | Registry | Coverage |
        |----------|----------|
        | Cardiovascular | MI, stroke, heart failure |
        | Cancer | All major cancer types |
        | Diabetes | Type 1, Type 2, gestational |
        | Renal | CKD stages, dialysis, transplant |
        """)
    
    with col_right:
        st.markdown("#### Example Phenotype Definition")
        
        with st.expander("🫀 Acute Myocardial Infarction (AMI)", expanded=True):
            st.markdown("""
            **Phenotype ID:** `acute_myocardial_infarction`  
            **Version:** 2.1.0
            
            **Data Sources:**
            - HES APC (diagnosis fields)
            - GDPPR (clinical events)
            
            **ICD-10 Codes:**
            `I21`, `I21.0`, `I21.1`, `I21.2`, `I21.3`, `I21.4`, `I21.9`, `I22`
            
            **SNOMED CT Codes:**
            `57054005`, `70422006`, `73795002`
            
            **Logic:** First occurrence of any matching code across sources, with date harmonisation and deduplication.
            """)
        
        with st.expander("🧠 Stroke"):
            st.markdown("""
            **Phenotype ID:** `stroke_any`  
            **Version:** 1.3.0
            
            **Data Sources:**
            - HES APC (primary and secondary diagnosis)
            - GDPPR (clinical events)
            - Stroke Registry
            
            **ICD-10 Codes:**
            `I60`, `I61`, `I62`, `I63`, `I64` (Haemorrhagic and Ischaemic)
            
            **Sub-phenotypes available:**
            - `stroke_ischaemic` (I63 only)
            - `stroke_haemorrhagic` (I60-I62)
            """)

# ----------------------------------------------------------------------------
# TAB 2: Partial Curation
# ----------------------------------------------------------------------------
with tab2:
    st.markdown("""
    <div class="service-intro">
        <h3>Partial Curation Service</h3>
        <p>Core ETL and harmonisation — transforming raw NHS data into clean, join-ready datasets.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### Included Transformations
        
        **Primary Care (GDPPR)**
        - Long-format event extraction
        - Wide-format pivoted features
        - Medication history compilation
        - Lab result harmonisation
        
        **Secondary Care (HES)**
        - Admitted Patient Care (APC) episodes
        - Outpatient appointments (OP)
        - Accident & Emergency (A&E)
        - Critical Care (CC)
        
        **Registries**
        - Cardiovascular disease registry
        - Cancer registry linkage
        - Mortality data (ONS)
        """)
    
    with col2:
        st.markdown("""
        #### Deliverables
        
        | Output | Format |
        |--------|--------|
        | Cleaned datasets | Delta Lake / Parquet |
        | Schema documentation | Markdown / HTML |
        | Data dictionary | Excel / JSON |
        | ETL notebooks | Databricks / Jupyter |
        | Lineage documentation | Mermaid diagrams |
        
        #### Quality Guarantees
        
        - ✅ Consistent date formats (ISO 8601)
        - ✅ Harmonised patient identifiers  
        - ✅ Standardised coding schemes
        - ✅ Null handling documentation
        - ✅ Versioned transformations
        """)
    
    st.markdown("#### Pipeline Architecture")
    
    # Visual pipeline using HTML/CSS (not Mermaid)
    st.markdown("""
    <div class="pipeline-visual">
        <div class="pipeline-stage stage-1">
            <div class="stage-icon">📥</div>
            <div class="stage-name">Ingest</div>
            <div class="stage-detail">Raw NHS data</div>
        </div>
        <div class="pipeline-connector">→</div>
        <div class="pipeline-stage stage-2">
            <div class="stage-icon">🧹</div>
            <div class="stage-name">Clean</div>
            <div class="stage-detail">Deduplication, nulls</div>
        </div>
        <div class="pipeline-connector">→</div>
        <div class="pipeline-stage stage-3">
            <div class="stage-icon">🔄</div>
            <div class="stage-name">Harmonise</div>
            <div class="stage-detail">Standardise schemas</div>
        </div>
        <div class="pipeline-connector">→</div>
        <div class="pipeline-stage stage-4">
            <div class="stage-icon">📦</div>
            <div class="stage-name">Output</div>
            <div class="stage-detail">Delta Lake tables</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Detailed pipeline diagram
    st.markdown("#### Detailed Data Flow")
    
    st.markdown("""
    <div class="detailed-pipeline">
        <div class="pipeline-row">
            <div class="source-group">
                <div class="source-label">Sources</div>
                <div class="source-items">
                    <span class="source-chip">Raw GDPPR</span>
                    <span class="source-chip">Raw HES</span>
                    <span class="source-chip">Registries</span>
                </div>
            </div>
            <div class="pipeline-arrow-large">⟶</div>
            <div class="process-group">
                <div class="process-label">Processing</div>
                <div class="process-items">
                    <span class="process-chip">Cleaning</span>
                    <span class="process-chip">Harmonisation</span>
                </div>
            </div>
            <div class="pipeline-arrow-large">⟶</div>
            <div class="output-group">
                <div class="output-label">Outputs</div>
                <div class="output-items">
                    <span class="output-chip">Long Format</span>
                    <span class="output-chip">Wide Format</span>
                </div>
            </div>
            <div class="pipeline-arrow-large">⟶</div>
            <div class="final-group">
                <div class="final-label">Storage</div>
                <div class="final-items">
                    <span class="final-chip">Delta Lake</span>
                    <span class="final-chip">Documentation</span>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ----------------------------------------------------------------------------
# TAB 3: Full Curation
# ----------------------------------------------------------------------------
with tab3:
    st.markdown("""
    <div class="service-intro">
        <h3>Full Curation Service</h3>
        <p>Everything in partial curation <strong>plus</strong> complete derivations layer — 
        delivering research-ready cohorts with derived covariates and outcomes.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="feature-grid">
        <div class="feature-card">
            <div class="feature-icon">📊</div>
            <h4>Covariate Derivation</h4>
            <ul>
                <li>Comorbidity indices (Charlson, Elixhauser)</li>
                <li>Medication exposure windows</li>
                <li>Lab-derived features (eGFR, HbA1c)</li>
                <li>Healthcare utilisation metrics</li>
            </ul>
        </div>
        <div class="feature-card">
            <div class="feature-icon">🎯</div>
            <h4>Outcome Derivation</h4>
            <ul>
                <li>Mortality (all-cause, CV, cancer)</li>
                <li>MACE (stroke, MI, CV death)</li>
                <li>Hospitalisation events</li>
                <li>Disease progression markers</li>
            </ul>
        </div>
        <div class="feature-card">
            <div class="feature-icon">⏱️</div>
            <h4>Temporal Features</h4>
            <ul>
                <li>Study windows (baseline, follow-up)</li>
                <li>Look-back period definitions</li>
                <li>Time-varying covariates</li>
                <li>Rolling aggregations</li>
            </ul>
        </div>
        <div class="feature-card">
            <div class="feature-icon">📋</div>
            <h4>Cohort Generation</h4>
            <ul>
                <li>Inclusion/exclusion criteria</li>
                <li>Index date assignment</li>
                <li>Cohort timeline generation</li>
                <li>Survival analysis datasets</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander("📐 Time-Varying Covariate Methodology"):
        st.markdown("""
        **Approach:** We derive time-varying covariates using configurable look-back windows.
        
        **Example: eGFR Features**
        
        For each patient, we calculate:
        - Latest eGFR value within 30, 90, and 365 days before index date
        - Mean eGFR across each window
        - Minimum eGFR (for detecting acute kidney injury)
        - Count of measurements (data density indicator)
        
        **Windows Available:**
        - Short-term: 30, 60, 90 days
        - Medium-term: 180, 365 days
        - Long-term: 2, 3, 5 years
        
        All derivations are documented with full methodology and validation checks.
        """)
    
    with st.expander("📊 MACE Outcome Methodology"):
        st.markdown("""
        **Definition:** Major Adverse Cardiovascular Events (MACE) is a composite outcome.
        
        **Components:**
        1. **Myocardial Infarction** — ICD-10 codes I21-I24 from HES APC
        2. **Stroke** — ICD-10 codes I60-I64 from HES APC
        3. **Cardiovascular Death** — ICD-10 Chapter I as underlying cause from ONS mortality
        
        **Derivation:**
        - Time to first MACE calculated from index date
        - Censoring at end of follow-up or non-CV death
        - Component-specific outcomes available separately
        
        **Output Fields:**
        - `mace_event` (binary)
        - `time_to_mace` (days)
        - `mace_type` (MI/Stroke/CV Death)
        - `mace_date`
        """)

# ----------------------------------------------------------------------------
# TAB 4: Data Quality
# ----------------------------------------------------------------------------
with tab4:
    st.markdown("""
    <div class="service-intro">
        <h3>Data Quality & Assurance</h3>
        <p>Comprehensive, automated quality checks ensuring your datasets meet research standards.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### Quality Dimensions
        
        | Dimension | Checks |
        |-----------|--------|
        | **Completeness** | Missingness rates, required fields |
        | **Validity** | Value ranges, code validity |
        | **Consistency** | Cross-table integrity, duplicates |
        | **Timeliness** | Date plausibility, sequence logic |
        | **Uniqueness** | Primary key checks, deduplication |
        | **Accuracy** | Distribution checks, outlier detection |
        """)
        
        st.markdown("""
        #### Automated Reports
        
        - 📄 PDF summary reports
        - 📊 HTML interactive dashboards  
        - 📋 Markdown documentation
        - 📈 Data quality scorecards
        """)
    
    with col2:
        st.markdown("#### Example Quality Metrics")
        
        # Simulated quality metrics
        quality_data = pd.DataFrame({
            "Table": ["gdppr_clean", "hes_apc_clean", "mortality", "cohort_final"],
            "Completeness": [98.2, 99.1, 99.8, 97.5],
            "Validity": [99.5, 98.8, 99.9, 99.2],
            "Consistency": [97.8, 99.2, 99.5, 98.1],
            "Overall Score": [98.5, 99.0, 99.7, 98.3]
        })
        
        st.dataframe(
            quality_data.style.background_gradient(cmap="RdYlGn", subset=["Completeness", "Validity", "Consistency", "Overall Score"]),
            hide_index=True,
            use_container_width=True
        )
        
        st.markdown("#### Completeness by Field")
        
        completeness_data = pd.DataFrame({
            "Field": ["nhs_number", "date", "code", "value", "unit"],
            "Present (%)": [100.0, 99.8, 99.5, 87.2, 72.1]
        })
        
        st.bar_chart(completeness_data.set_index("Field"), height=200)
    
    # Link to dashboard
    st.markdown("---")
    st.markdown("""
    <div class="dashboard-link">
        <h4>📊 Data Summary Dashboard</h4>
        <p>For comprehensive summary statistics across our curated datasets, visit the BHF Data Science Centre Dashboard:</p>
        <a href="https://bhfdatasciencecentre.org/dashboard/" target="_blank" class="dashboard-button">
            View Data Dashboard →
        </a>
    </div>
    """, unsafe_allow_html=True)

# ----------------------------------------------------------------------------
# TAB 5: Technical Stack
# ----------------------------------------------------------------------------
with tab5:
    st.markdown("""
    <div class="service-intro">
        <h3>Technical Capabilities</h3>
        <p>Enterprise-grade data engineering and analytics infrastructure.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="tech-card">
            <h4>🐍 Programming</h4>
            <div class="tech-list">
                <span class="tech-badge primary">PySpark</span>
                <span class="tech-badge primary">Python</span>
                <span class="tech-badge primary">R</span>
                <span class="tech-badge secondary">SQL</span>
                <span class="tech-badge secondary">Bash</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="tech-card">
            <h4>☁️ Platforms</h4>
            <div class="tech-list">
                <span class="tech-badge primary">Databricks</span>
                <span class="tech-badge primary">SAIL Databank</span>
                <span class="tech-badge secondary">Azure</span>
                <span class="tech-badge secondary">GitHub Actions</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="tech-card">
            <h4>📊 Data Formats</h4>
            <div class="tech-list">
                <span class="tech-badge primary">Delta Lake</span>
                <span class="tech-badge primary">Parquet</span>
                <span class="tech-badge secondary">CSV</span>
                <span class="tech-badge secondary">JSON</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("#### Databricks & Workflow Automation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Development Workflow**
        - Notebook-to-production using Databricks Repos
        - Git integration with feature branches
        - Code review for phenotypes and ETL logic
        - Parameterised notebooks for reproducibility
        
        **Performance Optimisation**
        - Delta Lake ZORDER clustering
        - OPTIMIZE for file compaction
        - Adaptive query execution
        - Broadcast joins for lookup tables
        """)
    
    with col2:
        st.markdown("""
        **CI/CD Pipeline**
        - Automated testing on PR
        - Linting and type checking
        - Integration tests with sample data
        - Deployment to production workspace
        
        **Job Orchestration**
        - Databricks Workflows
        - Parameterised job runs
        - Alerting and monitoring
        - Retry logic and failure handling
        """)
    
    st.markdown("#### Clinical Coding Systems")
    
    coding_systems = {
        "System": ["ICD-10", "SNOMED CT", "OPCS-4", "Read v2", "BNF/dm+d"],
        "Domain": ["Diagnoses", "Clinical terms", "Procedures", "Primary care", "Medications"],
        "Source": ["HES", "GDPPR", "HES procedures", "Legacy GP", "Prescriptions"],
        "Example Code": ["I21.0", "57054005", "K40.1", "G30..", "0407010F0"]
    }
    
    st.table(pd.DataFrame(coding_systems))

# ----------------------------------------------------------------------------
# TAB 6: Data Linkage
# ----------------------------------------------------------------------------
with tab6:
    st.markdown("""
    <div class="service-intro">
        <h3>Data Linkage Support</h3>
        <p>Harmonised patient-level timelines across multiple NHS data sources.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### Linkage Capabilities
        
        - **Deterministic linkage** using existing NHS join keys
        - **Cross-source harmonisation** of patient identifiers
        - **Temporal alignment** of events across sources
        - **Conflict resolution** for overlapping events
        - **Linkage quality metrics** and success rates
        
        #### Supported Linkages
        
        | Primary | Secondary |
        |---------|-----------|
        | GDPPR | HES APC |
        | GDPPR | HES OP |
        | GDPPR | HES A&E |
        | Any | ONS Mortality |
        | Any | Disease Registries |
        """)
    
    with col2:
        st.markdown("""
        #### Output Formats
        
        **Linked Patient Histories**
        - Unified timeline per patient
        - Source attribution for each event
        - Date harmonisation across sources
        
        **Linkage Metadata**
        - Match rates by source combination
        - Unlinked record counts
        - Data quality indicators
        
        **Documentation**
        - Linkage methodology description
        - Key mapping documentation
        - Limitation statements
        """)
    
    st.markdown("#### Linkage Architecture")
    
    st.markdown("""
    <div class="linkage-diagram">
        <div class="source-box">
            <div class="source-title">Primary Care</div>
            <div class="source-item">GDPPR</div>
        </div>
        <div class="source-box">
            <div class="source-title">Secondary Care</div>
            <div class="source-item">HES APC</div>
            <div class="source-item">HES OP</div>
            <div class="source-item">HES A&E</div>
        </div>
        <div class="source-box">
            <div class="source-title">Registries</div>
            <div class="source-item">CVD Registry</div>
            <div class="source-item">Cancer Registry</div>
        </div>
        <div class="source-box">
            <div class="source-title">Outcomes</div>
            <div class="source-item">ONS Mortality</div>
        </div>
        <div class="linkage-hub">
            <div class="hub-title">Linkage Hub</div>
            <div class="hub-detail">NHS Number / Token</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# USE CASES
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>Example Use Cases</h2>
    <p>Real-world applications of our data curation services</p>
</div>
""", unsafe_allow_html=True)

use_cases = [
    {
        "icon": "🫀",
        "title": "Cardiovascular Cohort with Time-Varying Covariates",
        "description": "Building a research-ready cohort for studying heart failure outcomes with longitudinal lab values and medication exposures.",
        "inputs": ["GDPPR (10 years)", "HES APC", "ONS Mortality", "CVD Registry"],
        "outputs": ["50,000 patient cohort", "200+ derived features", "Survival analysis dataset"],
        "pain_points": ["Manual lab harmonisation", "Inconsistent medication coding", "Complex temporal logic"]
    },
    {
        "icon": "🎗️",
        "title": "Cancer Incidence Study",
        "description": "Combining cancer registry data with hospital episodes to study diagnostic pathways and treatment patterns.",
        "inputs": ["Cancer Registry", "HES APC/OP", "GDPPR", "Mortality"],
        "outputs": ["Incidence cohort", "Diagnostic timeline", "Treatment sequence data"],
        "pain_points": ["Registry-HES date discrepancies", "Staging data completeness", "Recurrence definition"]
    },
    {
        "icon": "💊",
        "title": "Medication Safety Study",
        "description": "Using long-format GDPPR to study adverse events associated with specific medication classes.",
        "inputs": ["GDPPR prescriptions", "GDPPR events", "HES admissions"],
        "outputs": ["Exposure cohort", "Adverse event flags", "Propensity scores"],
        "pain_points": ["dm+d to BNF mapping", "Dose calculation", "Exposure window definition"]
    },
    {
        "icon": "📉",
        "title": "Mortality & Survival Analysis",
        "description": "Creating time-to-event datasets for all-cause and cause-specific mortality analysis.",
        "inputs": ["Cohort definition", "ONS Mortality", "HES last activity"],
        "outputs": ["Survival dataset", "Competing risks data", "Censor date logic"],
        "pain_points": ["Censoring logic complexity", "Loss to follow-up", "Cause of death coding"]
    }
]

cols = st.columns(2)
for i, uc in enumerate(use_cases):
    with cols[i % 2]:
        st.markdown(f"""
        <div class="use-case-card">
            <div class="use-case-header">
                <span class="use-case-icon">{uc['icon']}</span>
                <h4>{uc['title']}</h4>
            </div>
            <p>{uc['description']}</p>
            <div class="use-case-details">
                <div class="detail-section">
                    <strong>Inputs:</strong> {', '.join(uc['inputs'])}
                </div>
                <div class="detail-section">
                    <strong>Outputs:</strong> {', '.join(uc['outputs'])}
                </div>
                <div class="detail-section pain-points">
                    <strong>Pain Points Solved:</strong> {', '.join(uc['pain_points'])}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# FAQ
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>Frequently Asked Questions</h2>
    <p>Common questions about our data science services</p>
</div>
""", unsafe_allow_html=True)

faqs = [
    ("What datasets are supported?", 
     "We work with all major NHS England datasets including GDPPR (primary care), HES (secondary care including APC, OP, A&E, CC), ONS mortality, and disease registries. We also support SAIL Databank and other TRE environments."),
    
    ("What formats do you return cohorts in?",
     "Standard delivery is in Delta Lake format for Databricks environments, or Parquet for other platforms. We can also export to CSV or provide data in the native format of your TRE."),
    
    ("How are phenotypes versioned?",
     "All phenotype definitions are stored in Git with semantic versioning. Each phenotype includes a version number, changelog, and audit trail. Changes to codelists or logic are tracked and documented."),
    
    ("What programming languages are supported?",
     "Primary development is in PySpark for scalability. We also support R (using sparklyr/dbplyr) and can provide equivalent implementations. All phenotypes include SQL-compatible logic specifications."),
    
    ("Can I customise the phenotypes?",
     "Absolutely. Phenotypes are delivered as modular, configurable components. You can adjust codelists, date ranges, inclusion criteria, and logic without rewriting the pipeline."),
    
    ("How often can I re-run the pipeline?",
     "Pipelines are designed for repeated execution. With Delta Lake, we support incremental updates, so re-runs only process new data. Full refreshes are also supported for complete reproducibility."),
    
    ("What secure environments do you work in?",
     "We have experience in NHS Digital (now NHS England), SAIL Databank, OpenSAFELY, and various institutional TREs. Pipelines are designed to work within data governance constraints."),
    
    ("How long does a typical curation project take?",
     "Partial curation typically takes 2-4 weeks depending on data complexity. Full curation with derivations can take 4-8 weeks. Phenotype development is usually 1-2 weeks per phenotype.")
]

for q, a in faqs:
    with st.expander(q):
        st.write(a)

st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

# ============================================================================
# RAG Q&A SECTION - Using ccurag
# ============================================================================

st.markdown("""
<div class="section-header">
    <h2>💬 Ask a Question</h2>
    <p>Powered by <a href="https://github.com/zwelshman/ccurag/tree/claude/rag-app-017frFm55631qUUGSQqqZXFJ" target="_blank">ccurag</a> — Hybrid BM25 + Semantic Search with Claude</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="qa-intro">
    <p>Ask questions about our services, phenotype definitions, curation processes, or technical capabilities. 
    Our AI assistant uses hybrid search (BM25 keyword matching + vector semantic search) to retrieve relevant documentation and provide accurate answers.</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state for chat
if "messages" not in st.session_state:
    st.session_state.messages = []

# Chat interface
chat_container = st.container()

with chat_container:
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "sources" in message and message["sources"]:
                with st.expander("📚 Sources"):
                    for source in message["sources"]:
                        st.markdown(f"- {source}")

# User input
user_question = st.chat_input("Ask about phenotypes, curation, data quality...")

if user_question:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": user_question})
    
    with st.chat_message("user"):
        st.markdown(user_question)
    
    # Generate response (placeholder - would connect to actual ccurag system)
    with st.chat_message("assistant"):
        with st.spinner("Searching knowledge base..."):
            # Simulated RAG response
            # In production, this would call the ccurag hybrid retriever and Claude
            
            response_map = {
                "phenotype": """Based on our documentation, **phenotypes** are clinical definitions that identify patient populations or conditions using standardised clinical codes. 

Our phenotype development service includes:
- **Codelist-driven definitions** using SNOMED CT, ICD-10, OPCS-4, Read v2, and BNF
- **Version-controlled specifications** with full audit trails
- **Multi-source compatibility** across GDPPR, HES, and disease registries

Each phenotype is delivered as a structured JSON specification with full methodology documentation.""",

                "quality": """Our **Data Quality & Assurance** service provides comprehensive automated checks across six dimensions:

1. **Completeness** - Missingness rates and required field validation
2. **Validity** - Value range checks and code validity
3. **Consistency** - Cross-table integrity and duplicate detection
4. **Timeliness** - Date plausibility and sequence logic
5. **Uniqueness** - Primary key validation
6. **Accuracy** - Distribution analysis and outlier detection

Reports are delivered in PDF, HTML, and Markdown formats with interactive visualisations.

For comprehensive summary statistics, visit the [Data Summary Dashboard](https://bhfdatasciencecentre.org/dashboard/).""",

                "default": """I can help you with questions about:
- **Phenotype development** - Clinical definitions and codelists
- **Data curation** - ETL, harmonisation, and derivations
- **Data quality** - Automated QA processes
- **Technical capabilities** - PySpark, Databricks, coding systems
- **Data linkage** - Cross-source patient matching

What would you like to know more about?"""
            }
            
            # Simple keyword matching for demo
            response = response_map["default"]
            sources = []
            
            if "phenotype" in user_question.lower():
                response = response_map["phenotype"]
                sources = ["phenotype_development.md", "codelist_standards.md"]
            elif "quality" in user_question.lower() or "qa" in user_question.lower():
                response = response_map["quality"]
                sources = ["data_quality_framework.md", "qa_checklist.md"]
            
            st.markdown(response)
            
            if sources:
                with st.expander("📚 Sources"):
                    for source in sources:
                        st.markdown(f"- {source}")
            
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response,
                "sources": sources
            })

# RAG configuration note
with st.expander("⚙️ About the Q&A System"):
    st.markdown("""
    **Powered by [ccurag](https://github.com/zwelshman/ccurag/tree/claude/rag-app-017frFm55631qUUGSQqqZXFJ)**
    
    This Q&A system uses a hybrid Retrieval-Augmented Generation (RAG) approach:
    
    **Search Method:**
    - **BM25 Keyword Search** — Excellent for exact term matching (function names, clinical codes, identifiers)
    - **Vector Semantic Search** — Understands meaning and context for conceptual queries
    - **Adaptive Weighting** — Automatically adjusts based on query type
    
    **Technology Stack:**
    - Vector Database: Pinecone
    - Embeddings: BAAI/llm-embedder
    - Generation: Anthropic Claude
    - Hybrid Search: BM25 + Vector with score fusion
    
    **Query Flow:**
    
    Question → Hybrid Search (BM25 + Vector) → Top Documents → Claude → Answer + Sources
    """)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("""
<div class="footer">
    <div class="footer-content">
        <div class="footer-section">
            <h4>BHF Data Science Centre</h4>
            <p>Health Data Service — Transforming raw health records into research-ready datasets.</p>
        </div>
        <div class="footer-section">
            <h4>Resources</h4>
            <p><a href="https://bhfdatasciencecentre.org/dashboard/" target="_blank">Data Summary Dashboard</a></p>
            <p><a href="https://bhfdsc.github.io/documentation/" target="_blank">Health Data Science Resources</a></p>
            <p><a href="https://github.com/BHFDSC" target="_blank">GitHub Organisation</a></p>
        </div>
        <div class="footer-section">
            <h4>Built With</h4>
            <p>Streamlit • PySpark • Databricks • Delta Lake</p>
        </div>
    </div>
    <div class="footer-bottom">
        <p>© 2025 BHF Data Science Centre | <a href="https://bhfdatasciencecentre.org" target="_blank">bhfdatasciencecentre.org</a></p>
    </div>
</div>
""", unsafe_allow_html=True)
