import json
import logging
import os
import re
import hashlib
from typing import Dict, Any, List

import pandas as pd
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy.exc import SQLAlchemyError

from app.services.profiler import profile_dataframe
from app.db.database import SessionLocal
from app.db.models import AnalysisMemory
from app.utils.llm_utils import with_llm_retry, enforce_token_budget
from app.utils.security import sanitize_markdown_output

logger = logging.getLogger(__name__)

# Safe monkey-patch for langchain-core backwards compatibility issues
try:
    import langchain
    if not hasattr(langchain, "debug"):
        langchain.debug = False
    if not hasattr(langchain, "verbose"):
        langchain.verbose = False
    if not hasattr(langchain, "llm_cache"):
        langchain.llm_cache = None
except ImportError:
    pass

# ── LLM configuration ────────────────────────────────────────────────────────

LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
# Guard against decommissioned models or typos
if "llama-3.1-70b-versatile" in LLM_MODEL.lower() or "grok" in LLM_MODEL.lower():
    LLM_MODEL = "llama-3.3-70b-versatile"

LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))

# Privacy mode flags (mirrored from main.py via environment)
DISABLE_DATA_PERSISTENCE = os.getenv("DISABLE_DATA_PERSISTENCE", "false").lower() == "true"
ENABLE_DATA_MASKING = os.getenv("ENABLE_DATA_MASKING", "false").lower() == "true"

# Lazy-loaded LLM instance
_llm_instance = None


def _build_llm():
    """
    Build Groq LLM client.
    Reads GROQ_API_KEY (primary) with XAI_API_KEY as backward-compat fallback.
    """
    api_key = os.getenv("GROQ_API_KEY") or os.getenv("XAI_API_KEY", "")
    if not api_key:
        raise ValueError(
            "GROQ_API_KEY environment variable is required for LLM-powered analysis. "
            "Set it in your .env file."
        )
    return ChatGroq(
        model=LLM_MODEL,
        temperature=LLM_TEMPERATURE,
        api_key=api_key
    )


def get_llm():
    """Get or create the LLM instance (lazy-loaded, module-level singleton)."""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = _build_llm()
    return _llm_instance


# ── Allowed analysis operations (whitelist) ──────────────────────────────────

ALLOWED_OPERATIONS = {
    "missing_values",
    "describe_numeric",
    "value_counts",
    "correlation_matrix",
    "groupby_agg",
    "anomaly_detection",
    "time_series_trend"
}


# ── DataFrame loading ─────────────────────────────────────────────────────────

def _load_df(file_path: str) -> pd.DataFrame:
    """Load DataFrame from a CSV file on disk."""
    return pd.read_csv(file_path)


def _get_dataframe(state: Dict) -> pd.DataFrame:
    """
    Load the DataFrame for a given agent state.
    - If DISABLE_DATA_PERSISTENCE is active and a dataset is in the in-memory store,
      return it from there (never touches disk).
    - Otherwise fall back to file_path.
    """
    job_id = state.get("job_id", "")
    file_path = state.get("file_path", "")

    # Secure mode: check in-memory store first
    if DISABLE_DATA_PERSISTENCE and job_id:
        from app.utils.data_store import get_dataset
        df = get_dataset(job_id)
        if df is not None:
            return df
        # Sentinel check — file_path was set to memory://<job_id> but data is gone
        if file_path.startswith("memory://"):
            raise RuntimeError(
                f"In-memory dataset for job {job_id} was already discarded or never stored."
            )

    # Legacy mode: load from disk
    return _load_df(file_path)


# ── JSON / schema helpers ─────────────────────────────────────────────────────

def _to_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, (pd.Series, pd.DataFrame)):
        return _to_jsonable(value.to_dict())
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _safe_identifier(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""))


def _clean_json_text(content: str) -> str:
    clean = content.strip()
    if clean.startswith("```json"):
        return clean[7:-3].strip()
    if clean.startswith("```"):
        return clean[3:-3].strip()
    return clean


def _schema_fingerprint(profile: Dict[str, Any]) -> str:
    columns = profile.get("columns", {})
    normalized = [f"{name}:{meta.get('type', 'unknown')}" for name, meta in sorted(columns.items())]
    return hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()


# ── Memory ────────────────────────────────────────────────────────────────────

def _recent_memory_context(schema_fp: str, limit: int = 3) -> str:
    """Retrieve prior insights for the same schema fingerprint (safe, no raw data)."""
    db = SessionLocal()
    try:
        rows = (
            db.query(AnalysisMemory)
            .filter(AnalysisMemory.schema_fingerprint == schema_fp)
            .order_by(AnalysisMemory.created_at.desc())
            .limit(limit)
            .all()
        )
    except SQLAlchemyError:
        logger.exception("Failed fetching historical memory context")
        rows = []
    finally:
        db.close()

    if not rows:
        return ""
    return "\n".join(f"Historical Insight {i}: {row.insights_summary}" for i, row in enumerate(rows, 1))


def _store_memory(job_id: str, schema_fp: str, insights: str) -> None:
    """Persist insight summary keyed by schema fingerprint (no raw data stored)."""
    summary = insights.strip()[:1500]
    db = SessionLocal()
    try:
        db.add(AnalysisMemory(
            job_id=job_id,
            schema_fingerprint=schema_fp,
            insights_summary=summary
        ))
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        logger.exception("Failed storing analysis memory for job %s", job_id)
    finally:
        db.close()


# ── Question generation ───────────────────────────────────────────────────────

def _default_questions(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols = list(profile.get("columns", {}).keys())
    first = cols[0] if cols else "target"
    second = cols[1] if len(cols) > 1 else first
    base = [
        f"Which factors are most associated with changes in {first}?",
        f"What are the strongest trends observed in {first} over time?",
        f"Which categories of {second} contribute most to variation?",
        "Where are the major anomalies or outliers in the dataset?",
        "What actionable recommendations emerge from key correlations?"
    ]
    return [
        {"question": q, "relevance_score": 90 - i, "significance_score": 88 - i, "rank": i + 1}
        for i, q in enumerate(base)
    ]


def _validate_questions(items: Any) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        raise ValueError("Questions must be a list")
    valid = []
    for item in items:
        if not isinstance(item, dict):
            continue
        q = str(item.get("question", "")).strip()
        if not q:
            continue
        try:
            rel = float(item.get("relevance_score", 50))
            sig = float(item.get("significance_score", 50))
        except Exception:
            rel, sig = 50.0, 50.0
        valid.append({"question": q, "relevance_score": rel, "significance_score": sig, "rank": 0})
    valid.sort(key=lambda x: (x["relevance_score"], x["significance_score"]), reverse=True)
    for i, item in enumerate(valid):
        item["rank"] = i + 1
    return valid[:10]


# ── Plan validation ───────────────────────────────────────────────────────────

def _default_plan_from_profile(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = profile.get("columns", {})
    numeric_cols = [c for c, m in columns.items() if "int" in str(m.get("type", "")) or "float" in str(m.get("type", ""))]
    categorical_cols = [c for c, m in columns.items() if "object" in str(m.get("type", "")) or "category" in str(m.get("type", ""))]
    datetime_cols = [c for c, m in columns.items() if "date" in str(m.get("type", "")).lower() or "time" in str(m.get("type", "")).lower()]

    plan: List[Dict[str, Any]] = [
        {"task": "Assess missing values", "operation": "missing_values", "params": {}},
        {"task": "Summarize numeric distributions", "operation": "describe_numeric", "params": {"columns": numeric_cols[:10]}},
        {"task": "Detect anomalies in key numeric features", "operation": "anomaly_detection", "params": {"column": numeric_cols[0] if numeric_cols else "", "z_threshold": 3.0}}
    ]
    if categorical_cols:
        plan.append({"task": f"Category distribution for {categorical_cols[0]}", "operation": "value_counts", "params": {"column": categorical_cols[0], "top_n": 10}})
    if len(numeric_cols) >= 2:
        plan.append({"task": "Compute correlations between numeric features", "operation": "correlation_matrix", "params": {"columns": numeric_cols[:10]}})
    if datetime_cols and numeric_cols:
        plan.append({"task": "Analyze time-series trend", "operation": "time_series_trend", "params": {"date_column": datetime_cols[0], "value_column": numeric_cols[0], "freq": "M"}})
    return plan[:7]


def _validate_plan(plan: Any) -> List[Dict[str, Any]]:
    if not isinstance(plan, list):
        raise ValueError("Plan must be a list")
    valid_plan: List[Dict[str, Any]] = []
    for i, step in enumerate(plan):
        if not isinstance(step, dict):
            continue
        task = str(step.get("task", "")).strip() or f"Task {i + 1}"
        operation = str(step.get("operation", "")).strip()
        params = step.get("params", {})
        if operation not in ALLOWED_OPERATIONS:
            continue
        if not isinstance(params, dict):
            continue
        if operation == "time_series_trend":
            freq = params.get("freq", "M")
            if freq not in {"D", "W", "M", "Q", "Y"}:
                params["freq"] = "M"
        valid_plan.append({"task": task, "operation": operation, "params": params})
    if plan and not valid_plan:
        raise ValueError("No valid analysis steps after validation")
    return valid_plan[:10]


# ── Safe execution engine ─────────────────────────────────────────────────────

def _execute_operation(df: pd.DataFrame, step: Dict[str, Any]) -> Any:
    """
    Execute a single whitelisted analysis operation.
    NEVER returns raw row-level data — only aggregated, safe outputs.
    """
    operation = step["operation"]
    params = step.get("params", {})

    if operation == "missing_values":
        return df.isna().sum().to_dict()

    if operation == "describe_numeric":
        columns = params.get("columns") or df.select_dtypes(include="number").columns.tolist()
        columns = [col for col in columns if isinstance(col, str) and col in df.columns]
        if not columns:
            return {"message": "No numeric columns available"}
        return df[columns].describe().to_dict()

    if operation == "value_counts":
        column = params.get("column")
        top_n = params.get("top_n", 10)
        if not isinstance(column, str) or column not in df.columns:
            return {"message": "Invalid column for value_counts"}
        if not isinstance(top_n, int) or top_n < 1 or top_n > 100:
            top_n = 10
        counts = df[column].value_counts().head(top_n).to_dict()
        return {str(k): int(v) for k, v in counts.items()}

    if operation == "correlation_matrix":
        columns = params.get("columns") or df.select_dtypes(include="number").columns.tolist()
        columns = [col for col in columns if isinstance(col, str) and col in df.columns]
        if len(columns) < 2:
            return {"message": "Not enough numeric columns for correlation"}
        return df[columns].corr().to_dict()

    if operation == "groupby_agg":
        by = params.get("by")
        target = params.get("target")
        agg = params.get("agg", "mean")
        if not isinstance(by, str) or not _safe_identifier(by) or by not in df.columns:
            return {"message": "Invalid group-by column"}
        if not isinstance(target, str) or not _safe_identifier(target) or target not in df.columns:
            return {"message": "Invalid target column"}
        if agg not in {"mean", "sum", "min", "max", "median", "count"}:
            agg = "mean"
        grouped = df.groupby(by)[target].agg(agg)
        return grouped.to_dict() if hasattr(grouped, "to_dict") else str(grouped)

    if operation == "anomaly_detection":
        column = params.get("column")
        z_threshold = params.get("z_threshold", 3.0)
        if not isinstance(column, str) or column not in df.columns:
            return {"message": "Invalid numeric column for anomaly detection"}
        if not pd.api.types.is_numeric_dtype(df[column]):
            return {"message": "Anomaly detection requires numeric column"}

        series = pd.to_numeric(df[column], errors="coerce")
        mean = series.mean()
        std = series.std()
        if std is None or std == 0 or pd.isna(std):
            return {"message": "Insufficient variance for anomaly detection"}
        z_scores = (series - mean) / std
        mask = z_scores.abs() >= float(z_threshold)
        anomalies = series[mask]

        # Aggregation-only output — NO raw row-level data returned (privacy compliance)
        return {
            "column": column,
            "threshold": float(z_threshold),
            "count": int(mask.sum()),
            "percentage": round(float(mask.mean()) * 100, 2),
            "min_anomaly": float(anomalies.min()) if not anomalies.empty else None,
            "max_anomaly": float(anomalies.max()) if not anomalies.empty else None,
            "mean_anomaly": float(anomalies.mean()) if not anomalies.empty else None,
            "std_anomaly": float(anomalies.std()) if len(anomalies) > 1 else None,
        }

    if operation == "time_series_trend":
        date_column = params.get("date_column")
        value_column = params.get("value_column")
        freq = params.get("freq", "M")
        if not isinstance(date_column, str) or date_column not in df.columns:
            return {"message": "Invalid date_column"}
        if not isinstance(value_column, str) or value_column not in df.columns:
            return {"message": "Invalid value_column"}
        ts_df = df[[date_column, value_column]].copy()
        ts_df[date_column] = pd.to_datetime(ts_df[date_column], errors="coerce")
        ts_df[value_column] = pd.to_numeric(ts_df[value_column], errors="coerce")
        ts_df = ts_df.dropna(subset=[date_column, value_column])
        if ts_df.empty:
            return {"message": "No valid time-series rows"}
        trend = ts_df.set_index(date_column)[value_column].resample(freq).mean().dropna()
        return {
            "date_column": date_column,
            "value_column": value_column,
            "freq": freq,
            "points": [{"x": idx.isoformat(), "y": float(val)} for idx, val in trend.items()]
        }

    return {"message": f"Unsupported operation: {operation}"}


# ── Visualization metadata builder ────────────────────────────────────────────

def _build_visualizations(df: pd.DataFrame, execution_results: Dict[str, Any]) -> Dict[str, Any]:
    chart_specs: List[Dict[str, Any]] = []
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    category_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    if numeric_cols:
        first_num = numeric_cols[0]
        chart_specs.append({
            "id": "histogram_numeric",
            "title": f"Distribution of {first_num}",
            "data": [{"x": df[first_num].dropna().tolist(), "type": "histogram", "name": first_num}],
            "layout": {"xaxis": {"title": first_num}, "yaxis": {"title": "Count"}}
        })
        chart_specs.append({
            "id": "box_numeric",
            "title": f"Box Plot of {first_num}",
            "data": [{"y": df[first_num].dropna().tolist(), "type": "box", "name": first_num}],
            "layout": {"yaxis": {"title": first_num}}
        })

    if category_cols:
        first_cat = category_cols[0]
        top_counts = df[first_cat].value_counts().head(10)
        chart_specs.append({
            "id": "bar_category",
            "title": f"Top Categories in {first_cat}",
            "data": [{"x": [str(i) for i in top_counts.index.tolist()], "y": top_counts.values.tolist(), "type": "bar", "name": first_cat}],
            "layout": {"xaxis": {"title": first_cat}, "yaxis": {"title": "Count"}}
        })

    if len(numeric_cols) >= 2:
        corr = df[numeric_cols[:10]].corr()
        chart_specs.append({
            "id": "correlation_heatmap",
            "title": "Correlation Heatmap",
            "data": [{"z": corr.values.tolist(), "x": corr.columns.tolist(), "y": corr.index.tolist(), "type": "heatmap", "colorscale": "Viridis"}],
            "layout": {"xaxis": {"title": "Features"}, "yaxis": {"title": "Features"}}
        })

    # Anomaly scatter — uses only aggregated stats now, not raw rows
    anomaly_key = next((k for k in execution_results if "anomaly_detection" in k), "")
    if anomaly_key:
        anomaly_result = execution_results.get(anomaly_key, {})
        if isinstance(anomaly_result, dict) and anomaly_result.get("count", 0) > 0:
            col = anomaly_result.get("column", "value")
            chart_specs.append({
                "id": "anomaly_summary",
                "title": f"Anomaly Summary for {col}",
                "data": [{
                    "x": ["Min Anomaly", "Mean Anomaly", "Max Anomaly"],
                    "y": [
                        anomaly_result.get("min_anomaly"),
                        anomaly_result.get("mean_anomaly"),
                        anomaly_result.get("max_anomaly")
                    ],
                    "type": "bar", "name": col
                }],
                "layout": {"xaxis": {"title": "Stat"}, "yaxis": {"title": col}}
            })

    trend_key = next((k for k in execution_results if "time_series_trend" in k), "")
    if trend_key:
        trend_result = execution_results.get(trend_key, {})
        if isinstance(trend_result, dict) and trend_result.get("points"):
            x_values = [p.get("x") for p in trend_result["points"]]
            y_values = [p.get("y") for p in trend_result["points"]]
            val_col = trend_result.get("value_column", "value")
            chart_specs.append({
                "id": "line_timeseries",
                "title": f"Time-series Trend of {val_col}",
                "data": [{"x": x_values, "y": y_values, "mode": "lines+markers", "type": "scatter", "name": val_col}],
                "layout": {"xaxis": {"title": "Time"}, "yaxis": {"title": val_col}}
            })

    return {"chart_specs": chart_specs}


# ── Report builders ───────────────────────────────────────────────────────────

def _build_html_report(report: Dict[str, Any], html_path: str) -> None:
    questions = report.get("analytical_questions", [])
    question_items = "".join(
        [f"<li><strong>#{q.get('rank', '')}</strong> {q.get('question', '')}</li>" for q in questions]
    )
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Analysis Report {report.get('job_id')}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; line-height: 1.6; }}
    h1, h2 {{ color: #1f2937; }}
    pre {{ background: #f3f4f6; padding: 12px; border-radius: 8px; overflow-x: auto; }}
    .section {{ margin-bottom: 24px; }}
    .badge {{ background: #e0f2fe; color: #0369a1; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
  </style>
</head>
<body>
  <h1>Autonomous AI Data Intelligence Report <span class="badge">Privacy-First</span></h1>
  <div class="section"><h2>Job ID</h2><p>{report.get('job_id')}</p></div>
  <div class="section"><h2>Analytical Questions</h2><ol>{question_items}</ol></div>
  <div class="section"><h2>Insights</h2><pre>{report.get('insights', '')}</pre></div>
  <div class="section"><h2>Recommendations</h2><pre>{json.dumps(report.get('recommendations', []), indent=2)}</pre></div>
  <div class="section"><h2>Profile Summary</h2><pre>{json.dumps(report.get('profile', {}), indent=2)}</pre></div>
  <div class="section"><h2>Plan</h2><pre>{json.dumps(report.get('plan', []), indent=2)}</pre></div>
  <div class="section"><h2>Results</h2><pre>{json.dumps(report.get('results', {}), indent=2)}</pre></div>
  <div class="section"><h2>Visualization Metadata</h2><pre>{json.dumps(report.get('visualizations', {}), indent=2)}</pre></div>
</body>
</html>"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

def _build_pdf_report(report: Dict[str, Any], pdf_path: str) -> None:
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    y = height - 40

    def write_line(text: str) -> None:
        nonlocal y
        if y < 40:
            c.showPage()
            y = height - 40
        c.drawString(40, y, text)
        y -= 14

    write_line("Autonomous AI Data Intelligence Report (Privacy-First)")
    write_line(f"Job ID: {report.get('job_id')}")
    write_line(" ")
    write_line("Top Analytical Questions:")
    for q in report.get("analytical_questions", [])[:8]:
        write_line(f"- #{q.get('rank', '')} {q.get('question', '')[:120]}")
    write_line(" ")
    write_line("Insights:")
    for line in str(report.get("insights", "")).splitlines()[:40]:
        write_line(line[:120])
    write_line(" ")
    write_line("Recommendations:")
    for rec in report.get("recommendations", [])[:10]:
        write_line(f"- {str(rec)[:120]}")
    c.save()


# ── Agent nodes ───────────────────────────────────────────────────────────────

def profile_node(state: Dict) -> Dict:
    """
    Load dataset, optionally mask sensitive fields, then compute schema + statistics.
    Raw data is only held transiently in process memory during this call.
    """
    try:
        df = _get_dataframe(state)

        # Apply data masking if enabled (masks before any profiling occurs)
        if ENABLE_DATA_MASKING:
            from app.utils.security import mask_sensitive_dataframe
            df = mask_sensitive_dataframe(df)
            job_id = state.get("job_id", "")
            if DISABLE_DATA_PERSISTENCE:
                from app.utils.data_store import update_dataset
                if job_id:
                    update_dataset(job_id, df)
            else:
                # Ensure the masked data is written securely to disk for legacy mode downstream
                file_path = state.get("file_path", "")
                if file_path and os.path.exists(file_path):
                    df.to_csv(file_path, index=False)
            logger.info("Sensitive field masking applied for job %s", job_id)

        profile_data = profile_dataframe(df)
        return {"df_profile": profile_data}
    except Exception as e:
        logger.error("Error in profile_node: %s", e)
        return {"error": f"Failed to profile dataset: {e}"}


def question_node(state: Dict) -> Dict:
    try:
        profile_str = json.dumps(state.get("df_profile", {}), indent=2)
        profile_str = enforce_token_budget(profile_str, max_tokens=2000)

        prompt = PromptTemplate(
            template="""You are an expert Data Analyst.
Generate at least 5 analytical questions from this dataset profile.
Return ONLY JSON array where each item includes:
- question (string)
- relevance_score (0-100)
- significance_score (0-100)
Rank by business impact and data relevance.

Dataset Profile:
{profile}
""",
            input_variables=["profile"]
        )

        @with_llm_retry
        def _safe_invoke():
            return get_llm().invoke(prompt.format(profile=profile_str))

        response = _safe_invoke()
        raw = json.loads(_clean_json_text(response.content))
        questions = _validate_questions(raw)
        if len(questions) < 5:
            questions = _default_questions(state.get("df_profile", {}))
        return {"analytical_questions": questions}
    except Exception as e:
        logger.warning("Question generation failed, using fallback questions: %s", e)
        return {"analytical_questions": _default_questions(state.get("df_profile", {}))}


def plan_node(state: Dict) -> Dict:
    try:
        profile_str = enforce_token_budget(json.dumps(state.get("df_profile", {}), indent=2), max_tokens=1500)
        questions_str = enforce_token_budget(json.dumps(state.get("analytical_questions", []), indent=2), max_tokens=1000)

        prompt = PromptTemplate(
            template="""You are an expert Data Analyst Agent.
Given the dataset profile and ranked analytical questions, generate a multi-step analysis plan.
Use ONLY these operations:
missing_values, describe_numeric, value_counts, correlation_matrix, groupby_agg, anomaly_detection, time_series_trend.
Return ONLY JSON array where each item has:
- task (string)
- operation (one allowed operation)
- params (object)

Dataset Profile:
{profile}

Ranked Questions:
{questions}
""",
            input_variables=["profile", "questions"]
        )

        @with_llm_retry
        def _safe_invoke():
            return get_llm().invoke(prompt.format(profile=profile_str, questions=questions_str))

        response = _safe_invoke()
        raw_plan = json.loads(_clean_json_text(response.content))
        validated_plan = _validate_plan(raw_plan)
        return {"analysis_plan": validated_plan}
    except Exception as e:
        logger.warning("LLM plan generation failed, using safe default plan: %s", e)
        return {"analysis_plan": _default_plan_from_profile(state.get("df_profile", {}))}


def execute_node(state: Dict) -> Dict:
    try:
        df = _get_dataframe(state)
        plan = state.get("analysis_plan", [])
        results = {}

        for i, step in enumerate(plan):
            key = f"step_{i + 1}_{step.get('operation', 'unknown')}"
            try:
                results[key] = _execute_operation(df, step)
            except Exception as eval_err:
                logger.warning("Failed to execute step %d: %s", i, eval_err)
                results[f"step_{i + 1}_error"] = str(eval_err)

        return {"execution_results": _to_jsonable(results)}
    except Exception as e:
        logger.error("Error in execute_node: %s", e)
        return {"error": f"Failed to execute analysis plan: {e}"}


def insight_node(state: Dict) -> Dict:
    try:
        results_str = enforce_token_budget(json.dumps(state.get("execution_results", {}), indent=2), max_tokens=2000)
        questions_str = enforce_token_budget(json.dumps(state.get("analytical_questions", []), indent=2), max_tokens=1000)
        schema_fp = _schema_fingerprint(state.get("df_profile", {}))
        history_context = enforce_token_budget(_recent_memory_context(schema_fp), max_tokens=1000)

        prompt = PromptTemplate(
            template="""You are an expert Data Analyst Agent.
Write concise professional insights from the analysis results.
You must include:
- key trends
- correlations
- anomalies
- business recommendations

Ranked Questions:
{questions}

Analysis Results (aggregated, no raw data):
{results}

Relevant Historical Insights (if any):
{history}
""",
            input_variables=["results", "questions", "history"]
        )

        @with_llm_retry
        def _safe_invoke():
            return get_llm().invoke(
                prompt.format(results=results_str, questions=questions_str, history=history_context)
            )

        response = _safe_invoke()
        insights = sanitize_markdown_output(str(response.content))

        df = _get_dataframe(state)
        visualizations = _build_visualizations(df, state.get("execution_results", {}))

        import re
        recommendations = []
        in_recs_section = False
        for line in insights.splitlines():
            clean_line = line.strip()
            if not clean_line:
                continue
            if re.search(r'(?i)recommendation', clean_line):
                in_recs_section = True
                continue
            if in_recs_section and (clean_line.startswith('-') or clean_line.startswith('*') or re.match(r'^\d+\.', clean_line)):
                recommendations.append(clean_line.lstrip(' -*1234567890.'))
                
        if not recommendations:
            recommendations = [
                "Focus on high-variance features for segmentation.",
                "Investigate outlier records for process quality improvements.",
                "Track the strongest correlated metrics as KPI pairs."
            ]

        return {"insights": insights, "visualizations": visualizations, "recommendations": recommendations}
    except Exception as e:
        logger.error("Error in insight_node: %s", e)
        return {"error": f"Failed to generate insights: {e}"}


def report_node(state: Dict) -> Dict:
    """
    Build and persist the analysis report (JSON, HTML, PDF).
    After report files are written, the raw in-memory dataset is discarded
    so no user data lingers in process memory.
    """
    try:
        job_id = state["job_id"]
        schema_fp = _schema_fingerprint(state.get("df_profile", {}))

        report = {
            "job_id": job_id,
            "profile": state.get("df_profile", {}),
            "analytical_questions": state.get("analytical_questions", []),
            "plan": state.get("analysis_plan", []),
            "results": state.get("execution_results", {}),
            "insights": state.get("insights", ""),
            "visualizations": state.get("visualizations", {}),
            "recommendations": state.get("recommendations", [])
        }

        json_path = f"data/reports/{job_id}.json"
        html_path = f"data/reports/{job_id}.html"
        pdf_path = f"data/reports/{job_id}.pdf"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        _build_html_report(report, html_path)
        _build_pdf_report(report, pdf_path)

        # Store schema-fingerprinted insight summary (no raw data)
        _store_memory(job_id, schema_fp, str(report.get("insights", "")))

        # ── Discard raw dataset from memory — "Input → Analysis → Safe Insights → Discard Data"
        if DISABLE_DATA_PERSISTENCE:
            from app.utils.data_store import discard_dataset
            discard_dataset(job_id)
            logger.info("Raw dataset discarded from memory after report generation for job %s", job_id)

        return {
            "report_path": json_path,
            "report_paths": {"json": json_path, "html": html_path, "pdf": pdf_path}
        }
    except Exception as e:
        logger.error("Error in report_node: %s", e)
        return {"error": f"Failed to generate report: {e}"}


def clean_proposal_node(state: Dict) -> Dict:
    try:
        profile = state.get("df_profile", {})
        cols = profile.get("columns", {})
        missing_cols = [
            {"column": col, "missing": meta["num_missing"], "type": meta.get("type", "unknown")}
            for col, meta in cols.items()
            if meta.get("num_missing", 0) > 0
        ]
        if not missing_cols:
            return {"cleaning_plan": []}

        missing_str = enforce_token_budget(json.dumps(missing_cols, indent=2), max_tokens=1000)
        prompt = PromptTemplate(
            template="""You are an expert Data Engineer.
The following columns in the dataset have missing values:
{missing}

For each column, propose exactly one cleaning action from this allowed list:
- drop (drops rows with missing values)
- drop_column (drops the entire column if too much is missing)
- impute_mean (fills with the column's mean, numeric only)
- impute_median (fills with the column's median, numeric only)
- impute_mode (fills with the most frequent value)

Return ONLY a JSON array where each object has:
- column (string)
- action (string from the allowed list)
- message (string explaining why)
""",
            input_variables=["missing"]
        )

        @with_llm_retry
        def _safe_invoke():
            return get_llm().invoke(prompt.format(missing=missing_str))

        response = _safe_invoke()
        try:
            plan = json.loads(_clean_json_text(response.content))
            allowed = {"drop", "drop_column", "impute_mean", "impute_median", "impute_mode"}
            valid_plan = [
                step for step in plan
                if step.get("action") in allowed and step.get("column") in cols
            ]
            return {"cleaning_plan": valid_plan}
        except Exception as e:
            logger.warning("Failed to parse cleaning plan: %s", e)
            return {"cleaning_plan": []}
    except Exception as e:
        logger.error("Error in clean_proposal_node: %s", e)
        return {"error": f"Failed to propose cleaning plan: {e}"}


def execute_cleaning_node(state: Dict) -> Dict:
    """
    Apply the approved cleaning plan to the dataset.
    - Secure mode (DISABLE_DATA_PERSISTENCE): updates the in-memory store, never writes to disk.
    - Legacy mode: writes cleaned CSV back to file_path.
    """
    try:
        df = _get_dataframe(state)
        plan = state.get("cleaning_plan", [])

        if not plan:
            return {}

        modified = False
        for step in plan:
            action = step.get("action")
            col = step.get("column")
            if col not in df.columns:
                continue
            modified = True
            if action == "drop":
                df = df.dropna(subset=[col])
            elif action == "drop_column":
                df = df.drop(columns=[col])
            elif action == "impute_mean" and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].mean())
            elif action == "impute_median" and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].median())
            elif action == "impute_mode":
                mode_val = df[col].mode()
                if not mode_val.empty:
                    df[col] = df[col].fillna(mode_val.iloc[0])

        if modified:
            if DISABLE_DATA_PERSISTENCE:
                # Secure mode: update in-memory store only
                from app.utils.data_store import update_dataset
                update_dataset(state.get("job_id", ""), df)
                logger.info(
                    "Cleaned dataset updated in-memory for job %s (%d rows)",
                    state.get("job_id", ""), len(df)
                )
            else:
                # Legacy mode: persist cleaned CSV to disk
                df.to_csv(state["file_path"], index=False)

        return {}
    except Exception as e:
        logger.error("Error in execute_cleaning_node: %s", e)
        return {"error": f"Failed to execute cleaning: {e}"}
