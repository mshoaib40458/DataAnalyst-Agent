---
title: DataAnalyst Agent
emoji: 📊
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
---

# DataAnalyst Agent 🧠📊

**A Privacy-First, Autonomous Multi-Agent Data Analysis System**

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge\&logo=python\&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge\&logo=fastapi\&logoColor=white)
![LangGraph](https://img.shields.io/badge/LangGraph-D5A6BD?style=for-the-badge)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge\&logo=pandas\&logoColor=white)

---

## 📖 Overview

**DataAnalyst Agent** is a privacy-preserving, agentic AI system that performs autonomous data analysis using a structured **LangGraph multi-agent pipeline**.

The system ingests structured datasets (CSV/SQL), automatically profiles schema, generates analytical hypotheses, constructs deterministic execution plans, and produces human-readable insights — all without human intervention.

Designed with a **zero-retention architecture**, all data is processed strictly in-memory and securely cleared after execution, ensuring strong privacy guarantees.

---

## 🧠 Key Capabilities

* 📊 Automated dataset understanding (schema profiling)
* ❓ AI-generated analytical questions & hypotheses
* 🧮 Deterministic execution using Pandas (no hallucinated computation)
* 🧠 LLM-powered insight generation
* 🔐 Privacy-first processing (PII masking + zero retention)
* ⚡ Asynchronous execution (non-blocking API)
* 📄 Exportable reports (JSON / HTML / PDF)

---

## 💼 Real-World Use Cases

* Customer behavior analytics (without exposing PII)
* Financial reporting and summarization
* Automated exploratory data analysis (EDA)
* Internal enterprise analytics tools
* Privacy-sensitive datasets (healthcare, business intelligence)

---

## 🏗️ Architectural Flow (Simplified View)

```mermaid
graph TD;
    A[Frontend Dashboard] --> |Upload Request| B(FastAPI API Gateway)
    B --> C{Security & Validation Layer}
    C -->|Sanitized Data| D[(In-Memory DataFrame)]
    D --> E[LangGraph Orchestrator]
    
    subgraph Agent Pipeline
    E --> F[1. Schema Profiler]
    F --> G[2. Question Generator (LLM)]
    G --> H[3. Execution Planner (LLM)]
    H --> I[4. Sandboxed Python Execution]
    I --> J[5. Insight Generator (LLM)]
    end
    
    J --> K[Report Generator]
    K --> L[Memory Cleanup Daemon]
    L --> M[Results Returned to User]
```


---

## 🧠 Design Principles

* **Cognitive Isolation**: LLMs never access raw datasets directly
* **Deterministic Execution**: All computations handled via Python (Pandas)
* **Zero Data Persistence**: No dataset is written to disk
* **Separation of Concerns**: Clear boundaries between reasoning, execution, and storage
* **Fail-Safe Execution**: Sandboxed environment prevents unsafe operations

---

## ⚡ Core Engineering Highlights

### 🔹 Multi-Agent Orchestration (LangGraph)

Implements a structured pipeline:

* Schema Profiling → Question Generation → Execution Planning → Deterministic Execution → Insight Synthesis

### 🔹 Zero-Retention Architecture

Data is processed exclusively in-memory and automatically cleared after execution via a cleanup daemon.

### 🔹 Dynamic PII Masking

Sensitive fields are anonymized before any LLM interaction using regex-based detection and synthetic data replacement.

### 🔹 Asynchronous Processing

Built using **FastAPI BackgroundTasks**, enabling non-blocking execution and responsive APIs.

### 🔹 Secure Logging

Implements redacted logging to ensure sensitive data is never exposed in logs.

---

## 🚀 Quick Start Guide

### Prerequisites

* Python 3.10+
* Git

---

### 1. Clone the Repository

```bash
git clone https://github.com/mshoaib40458/DataAnalyst-Agent.git
cd DataAnalyst-Agent
```

---

### 2. Environment Configuration

```bash
cp .env.example .env
```

Add your API key:

```
GROQ_API_KEY=your_api_key_here
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Run the System

#### Backend (FastAPI)

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

#### Frontend (Flask)

```bash
cd frontend
python app.py
```

Access the dashboard at:

```
http://127.0.0.1:5000
```

---

## 🛠️ Configuration (.env)

| Variable                   | Description                                    |
| -------------------------- | ---------------------------------------------- |
| `LLM_MODEL`                | Model used for reasoning (e.g., llama-3.1-70b) |
| `ENABLE_DATA_MASKING`      | Enable/disable PII masking                     |
| `DISABLE_DATA_PERSISTENCE` | Enforce zero-retention                         |
| `MAX_UPLOAD_SIZE_BYTES`    | Limit dataset size                             |
| `PROXY_TRUST_MODE`         | Enable trusted proxy validation                |

---

## 🎯 System Highlights

* 🔐 Privacy-first AI system
* 🧠 Agentic architecture (LangGraph)
* ⚡ Async & scalable backend
* 🛡️ Secure execution environment
* 📊 Fully automated data analysis

---

## 🧠 One-Line Summary

> A privacy-preserving, agentic AI system that autonomously analyzes structured data using a controlled LangGraph pipeline with zero data retention.

---

## 📌 Future Improvements

* Distributed task queue (Celery / Redis)
* Vector memory for contextual recall
* Advanced visualization dashboard
* Multi-dataset comparative analysis

---

> *"Designing AI systems that are not only intelligent, but also secure, controlled, and production-ready."*
