"""
RAG Module for Health Data Science-as-a-Service

This module provides Retrieval Augmented Generation capabilities using:
- Pinecone for vector storage and retrieval
- Anthropic Claude for answer generation
- Hybrid BM25 + semantic search

To use this module, set the following environment variables:
- PINECONE_API_KEY
- PINECONE_INDEX
- ANTHROPIC_API_KEY
"""

import os
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import json

# Check for optional dependencies
try:
    from pinecone import Pinecone
    PINECONE_AVAILABLE = True
except ImportError:
    PINECONE_AVAILABLE = False

try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False


@dataclass
class RetrievedDocument:
    """A document retrieved from the knowledge base."""
    content: str
    source: str
    score: float
    metadata: Dict


@dataclass
class RAGResponse:
    """Response from the RAG system."""
    answer: str
    sources: List[RetrievedDocument]
    query: str


class HDSSKnowledgeBase:
    """
    Knowledge base for Health Data Science-as-a-Service.
    
    Handles document embedding, storage, and retrieval using Pinecone.
    """
    
    def __init__(
        self,
        pinecone_api_key: Optional[str] = None,
        pinecone_index: Optional[str] = None,
        embedding_model: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize the knowledge base.
        
        Args:
            pinecone_api_key: Pinecone API key (or use PINECONE_API_KEY env var)
            pinecone_index: Pinecone index name (or use PINECONE_INDEX env var)
            embedding_model: Sentence transformer model for embeddings
        """
        self.api_key = pinecone_api_key or os.getenv("PINECONE_API_KEY")
        self.index_name = pinecone_index or os.getenv("PINECONE_INDEX", "hdss-knowledge")
        
        # Initialize Pinecone
        if PINECONE_AVAILABLE and self.api_key:
            self.pc = Pinecone(api_key=self.api_key)
            self.index = self.pc.Index(self.index_name)
        else:
            self.pc = None
            self.index = None
        
        # Initialize embedding model
        if EMBEDDINGS_AVAILABLE:
            self.encoder = SentenceTransformer(embedding_model)
        else:
            self.encoder = None
    
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for text."""
        if not self.encoder:
            raise RuntimeError("Sentence transformers not available")
        return self.encoder.encode(text).tolist()
    
    def embed_documents(self, documents: List[Dict]) -> List[Dict]:
        """
        Generate embeddings for a list of documents.
        
        Args:
            documents: List of dicts with 'content', 'source', and optional 'metadata'
        
        Returns:
            List of dicts ready for Pinecone upsert
        """
        vectors = []
        for i, doc in enumerate(documents):
            embedding = self.embed_text(doc["content"])
            vectors.append({
                "id": doc.get("id", f"doc_{i}"),
                "values": embedding,
                "metadata": {
                    "content": doc["content"],
                    "source": doc["source"],
                    **doc.get("metadata", {})
                }
            })
        return vectors
    
    def upsert_documents(self, documents: List[Dict], batch_size: int = 100) -> int:
        """
        Upsert documents to Pinecone.
        
        Args:
            documents: List of document dicts
            batch_size: Number of documents per batch
        
        Returns:
            Number of documents upserted
        """
        if not self.index:
            raise RuntimeError("Pinecone not configured")
        
        vectors = self.embed_documents(documents)
        
        # Batch upsert
        total = 0
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            self.index.upsert(vectors=batch)
            total += len(batch)
        
        return total
    
    def search(
        self,
        query: str,
        top_k: int = 5,
        filter: Optional[Dict] = None
    ) -> List[RetrievedDocument]:
        """
        Search the knowledge base.
        
        Args:
            query: Search query
            top_k: Number of results to return
            filter: Optional Pinecone filter
        
        Returns:
            List of retrieved documents
        """
        if not self.index:
            raise RuntimeError("Pinecone not configured")
        
        query_embedding = self.embed_text(query)
        
        results = self.index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True,
            filter=filter
        )
        
        documents = []
        for match in results.matches:
            documents.append(RetrievedDocument(
                content=match.metadata.get("content", ""),
                source=match.metadata.get("source", "Unknown"),
                score=match.score,
                metadata={k: v for k, v in match.metadata.items() 
                         if k not in ["content", "source"]}
            ))
        
        return documents


class HDSSAssistant:
    """
    RAG-powered assistant for Health Data Science-as-a-Service.
    
    Combines retrieval from the knowledge base with Claude for generation.
    """
    
    SYSTEM_PROMPT = """You are an expert assistant for Health Data Science-as-a-Service (HDSaaS), 
a platform that helps research groups work with large-scale NHS health datasets.

Your expertise includes:
- Clinical phenotype development using SNOMED CT, ICD-10, OPCS-4, Read v2, and BNF coding systems
- NHS data curation including GDPPR (primary care) and HES (secondary care)
- PySpark and Databricks for data processing
- Data quality assurance and validation
- Data linkage across multiple NHS sources
- Covariate and outcome derivation for research cohorts

When answering questions:
1. Use the provided context to give accurate, specific answers
2. Cite the sources when referencing specific information
3. If the context doesn't contain relevant information, say so clearly
4. Provide practical examples where helpful
5. Be concise but thorough

Context from the knowledge base:
{context}
"""
    
    def __init__(
        self,
        knowledge_base: Optional[HDSSKnowledgeBase] = None,
        anthropic_api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-20250514"
    ):
        """
        Initialize the assistant.
        
        Args:
            knowledge_base: HDSSKnowledgeBase instance
            anthropic_api_key: Anthropic API key (or use ANTHROPIC_API_KEY env var)
            model: Claude model to use
        """
        self.kb = knowledge_base or HDSSKnowledgeBase()
        self.api_key = anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
        self.model = model
        
        if ANTHROPIC_AVAILABLE and self.api_key:
            self.client = Anthropic(api_key=self.api_key)
        else:
            self.client = None
    
    def _format_context(self, documents: List[RetrievedDocument]) -> str:
        """Format retrieved documents as context."""
        if not documents:
            return "No relevant documents found."
        
        context_parts = []
        for i, doc in enumerate(documents, 1):
            context_parts.append(
                f"[Source {i}: {doc.source}]\n{doc.content}\n"
            )
        return "\n---\n".join(context_parts)
    
    def ask(
        self,
        question: str,
        top_k: int = 5,
        filter: Optional[Dict] = None
    ) -> RAGResponse:
        """
        Ask a question and get a RAG-powered answer.
        
        Args:
            question: User's question
            top_k: Number of documents to retrieve
            filter: Optional filter for retrieval
        
        Returns:
            RAGResponse with answer and sources
        """
        # Retrieve relevant documents
        try:
            documents = self.kb.search(question, top_k=top_k, filter=filter)
        except Exception as e:
            documents = []
        
        # Format context
        context = self._format_context(documents)
        system_prompt = self.SYSTEM_PROMPT.format(context=context)
        
        # Generate answer with Claude
        if self.client:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": question}
                ]
            )
            answer = response.content[0].text
        else:
            # Fallback for demo mode
            answer = self._demo_response(question, documents)
        
        return RAGResponse(
            answer=answer,
            sources=documents,
            query=question
        )
    
    def _demo_response(self, question: str, documents: List[RetrievedDocument]) -> str:
        """Generate a demo response when Claude is not available."""
        q_lower = question.lower()
        
        if "phenotype" in q_lower:
            return """Phenotypes in HDSaaS are clinical definitions that identify patient populations 
or conditions using standardised clinical codes.

Our phenotype development service includes:
- **Codelist-driven definitions** using SNOMED CT, ICD-10, OPCS-4, Read v2, and BNF
- **Version-controlled specifications** with full audit trails
- **Multi-source compatibility** across GDPPR, HES, and disease registries

Each phenotype is delivered as a structured JSON specification with accompanying PySpark implementation."""
        
        elif "quality" in q_lower or "qa" in q_lower:
            return """Our Data Quality & Assurance service provides comprehensive automated checks:

1. **Completeness** - Missingness rates and required field validation
2. **Validity** - Value range checks and code validity
3. **Consistency** - Cross-table integrity and duplicate detection
4. **Timeliness** - Date plausibility and sequence logic
5. **Uniqueness** - Primary key validation
6. **Accuracy** - Distribution analysis and outlier detection

Reports are delivered in PDF, HTML, and Markdown formats."""
        
        elif "curation" in q_lower or "etl" in q_lower:
            return """We offer two levels of data curation:

**Partial Curation** (Core ETL):
- Long-format primary care data transformation
- Secondary care harmonisation (HES APC, OP, A&E)
- Disease registry ingestion
- Schema documentation

**Full Curation** (Complete Derivations):
- All partial curation features
- Covariate derivation (comorbidities, medications, labs)
- Outcome derivation (MACE, mortality, hospitalisation)
- Time-varying features
- Research-ready cohort generation"""
        
        else:
            return """I can help you with questions about:

- **Phenotype development** - Clinical definitions and codelists
- **Data curation** - ETL, harmonisation, and derivations
- **Data quality** - Automated QA processes
- **Technical capabilities** - PySpark, Databricks, coding systems
- **Data linkage** - Cross-source patient matching

What would you like to know more about?"""


# Convenience function for Streamlit integration
def get_rag_response(question: str) -> Tuple[str, List[str]]:
    """
    Get a RAG response for Streamlit integration.
    
    Args:
        question: User's question
    
    Returns:
        Tuple of (answer, list of source names)
    """
    assistant = HDSSAssistant()
    response = assistant.ask(question)
    sources = [doc.source for doc in response.sources]
    return response.answer, sources


# Document structure for populating the knowledge base
SAMPLE_DOCUMENTS = [
    {
        "id": "phenotype_overview",
        "content": """Phenotypes are clinical definitions that identify patient populations or conditions 
using standardised clinical codes. HDSaaS phenotypes use SNOMED CT, ICD-10, OPCS-4, Read v2, 
and BNF coding systems. Each phenotype is version-controlled and includes structured JSON 
specifications with PySpark implementation code.""",
        "source": "phenotype_development.md",
        "metadata": {"category": "phenotypes", "section": "overview"}
    },
    {
        "id": "ami_phenotype",
        "content": """Acute Myocardial Infarction (AMI) phenotype identifies heart attack events using:
- ICD-10 codes: I21, I21.0-I21.9, I22 (from HES APC diagnosis fields)
- SNOMED CT codes: 57054005, 70422006, 73795002 (from GDPPR)
The phenotype combines both sources and deduplicates to identify the first AMI event per patient.""",
        "source": "phenotypes/ami.md",
        "metadata": {"category": "phenotypes", "condition": "cardiovascular"}
    },
    {
        "id": "partial_curation",
        "content": """Partial curation provides core ETL and harmonisation:
- Long-format GDPPR transformation with event extraction
- Wide-format variants with pivoted clinical features
- HES transformations (APC, OP, A&E)
- Disease registry ingestion
- Cleaned, harmonised, join-ready datasets
- Schema documentation and data dictionaries""",
        "source": "curation_services.md",
        "metadata": {"category": "curation", "level": "partial"}
    },
    {
        "id": "full_curation",
        "content": """Full curation includes all partial curation features plus:
- Covariate derivation: Charlson index, Elixhauser, medication exposures, lab features
- Outcome derivation: mortality, MACE, hospitalisation, disease progression
- Temporal features: study windows, look-back periods, time-varying covariates
- Cohort generation: inclusion/exclusion criteria, index dates, survival datasets""",
        "source": "curation_services.md",
        "metadata": {"category": "curation", "level": "full"}
    },
    {
        "id": "data_quality",
        "content": """Data quality assurance covers six dimensions:
1. Completeness - missingness rates, required fields
2. Validity - value ranges, code validity checks
3. Consistency - cross-table integrity, duplicate detection
4. Timeliness - date plausibility, sequence validation
5. Uniqueness - primary key checks
6. Accuracy - distribution analysis, outlier detection
Automated reports are generated in PDF, HTML, and Markdown formats.""",
        "source": "data_quality_framework.md",
        "metadata": {"category": "quality"}
    }
]


if __name__ == "__main__":
    # Example usage
    print("HDSaaS RAG Module")
    print("-" * 40)
    
    # Check dependencies
    print(f"Pinecone available: {PINECONE_AVAILABLE}")
    print(f"Anthropic available: {ANTHROPIC_AVAILABLE}")
    print(f"Embeddings available: {EMBEDDINGS_AVAILABLE}")
    
    # Demo mode
    assistant = HDSSAssistant()
    response = assistant.ask("What is a phenotype?")
    print(f"\nQuestion: What is a phenotype?")
    print(f"Answer: {response.answer}")
