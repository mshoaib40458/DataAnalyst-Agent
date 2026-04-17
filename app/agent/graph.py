from langgraph.graph import StateGraph, END
from typing import TypedDict, Dict, Any, List
import logging
import os
from sqlalchemy.exc import SQLAlchemyError
from app.db.database import SessionLocal
from app.db.models import AnalysisJob
from app.agent.nodes import (
    profile_node, question_node, plan_node, execute_node,
    insight_node, report_node, clean_proposal_node, execute_cleaning_node
)

logger = logging.getLogger(__name__)
MAX_PROGRESS_LOG_CHARS = int(os.getenv("MAX_PROGRESS_LOG_CHARS", "20000"))


class AgentState(TypedDict):
    job_id: str
    file_path: str
    df_profile: Dict[str, Any]
    cleaning_plan: List[Dict[str, Any]]
    analytical_questions: List[Dict[str, Any]]
    analysis_plan: List[Dict[str, Any]]
    execution_results: Dict[str, Any]
    visualizations: Dict[str, Any]
    insights: str
    recommendations: List[str]
    report_path: str
    report_paths: Dict[str, str]
    error: str


# ── Progress logging ──────────────────────────────────────────────────────────

def _update_progress(job_id: str, new_log: str) -> None:
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if job:
            logs = (job.progress_logs or "") + new_log + "\n"
            if len(logs) > MAX_PROGRESS_LOG_CHARS:
                logs = logs[-MAX_PROGRESS_LOG_CHARS:]
            job.progress_logs = logs
            db.commit()
    except SQLAlchemyError as db_err:
        db.rollback()
        logger.error("Failed to update progress log for job %s: %s", job_id, db_err)
    finally:
        db.close()


def log_progress_node(state: AgentState, message: str) -> None:
    job_id = state["job_id"]
    _update_progress(job_id, f"[System]: {message}")
    logger.info("Job %s: %s", job_id, message)


# ── Node wrappers (add progress logging around each node) ─────────────────────

def profile_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Starting dataset profiling...")
    result = profile_node(state)
    log_progress_node(state, "Dataset profiling completed.")
    return result


def clean_proposal_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Analyzing dataset for missing values...")
    result = clean_proposal_node(state)
    log_progress_node(state, "Cleaning proposals generated.")
    return result


def execute_cleaning_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Executing approved data cleaning...")
    result = execute_cleaning_node(state)
    log_progress_node(state, "Data cleaning applied.")
    return result


def question_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Generating ranked analytical questions with LLM...")
    result = question_node(state)
    log_progress_node(state, "Analytical questions generated.")
    return result


def plan_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Generating analysis plan with LLM...")
    result = plan_node(state)
    log_progress_node(state, "Analysis plan generated.")
    return result


def execute_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Executing analysis operations (safe, aggregated only)...")
    result = execute_node(state)
    log_progress_node(state, "Analysis execution completed.")
    return result


def insight_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Generating insights and visualization metadata with LLM...")
    result = insight_node(state)
    log_progress_node(state, "Insights generated.")
    return result


def report_wrapper(state: AgentState) -> AgentState:
    log_progress_node(state, "Building final report (JSON, HTML, PDF)...")
    result = report_node(state)
    log_progress_node(state, "Report generated. Raw dataset discarded from memory.")
    return result


def error_handler(state: AgentState) -> AgentState:
    job_id = state["job_id"]
    error_msg = state.get("error", "Unknown error occurred")
    _update_progress(job_id, f"[Error]: {error_msg}")

    # Ensure in-memory dataset is discarded even on error
    from app.utils.data_store import discard_dataset
    discard_dataset(job_id)

    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if job:
            job.status = "error"
            job.error_message = error_msg
            db.commit()
    except SQLAlchemyError as db_err:
        db.rollback()
        logger.error("Failed to persist error state for job %s: %s", job_id, db_err)
    finally:
        db.close()
    return state


# ── Route functions ───────────────────────────────────────────────────────────

def _route_error_or(next_node: str):
    def route(state: AgentState) -> str:
        return "error_node" if state.get("error") else next_node
    return route


route_after_profile_cleaner = _route_error_or("clean_proposal")
route_after_cleaning = _route_error_or("profile")
route_after_profile = _route_error_or("question")
route_after_question = _route_error_or("plan")
route_after_plan = _route_error_or("execute")
route_after_execute = _route_error_or("insight")
route_after_insight = _route_error_or("report")


def route_after_report(state: AgentState) -> str:
    return "error_node" if state.get("error") else END


# ── UNIFIED AUTONOMOUS GRAPH (UPDATED README: Profile→Question→Plan→Execute→Insight→Report) ──
# This is the primary pipeline for start_analysis. Fully autonomous, no human gates.

_unified_builder = StateGraph(AgentState)
_unified_builder.add_node("profile", profile_wrapper)
_unified_builder.add_node("question", question_wrapper)
_unified_builder.add_node("plan", plan_wrapper)
_unified_builder.add_node("execute", execute_wrapper)
_unified_builder.add_node("insight", insight_wrapper)
_unified_builder.add_node("report", report_wrapper)
_unified_builder.add_node("error_node", error_handler)
_unified_builder.set_entry_point("profile")
_unified_builder.add_conditional_edges("profile", route_after_profile)
_unified_builder.add_conditional_edges("question", route_after_question)
_unified_builder.add_conditional_edges("plan", route_after_plan)
_unified_builder.add_conditional_edges("execute", route_after_execute)
_unified_builder.add_conditional_edges("insight", route_after_insight)
_unified_builder.add_conditional_edges("report", route_after_report)
_unified_builder.add_edge("error_node", END)
unified_workflow = _unified_builder.compile()



# ── Helper: build initial state from DB job ───────────────────────────────────

def _build_initial_state(job, extra: dict = None) -> AgentState:
    import json as _json
    state = AgentState(
        job_id=job.id,
        file_path=job.file_path,
        df_profile={},
        cleaning_plan=_json.loads(job.cleaning_plan) if job.cleaning_plan else [],
        analytical_questions=_json.loads(job.analytical_questions) if job.analytical_questions else [],
        analysis_plan=_json.loads(job.analysis_plan) if job.analysis_plan else [],
        execution_results={},
        visualizations={},
        insights="",
        recommendations=[],
        report_path="",
        report_paths={},
        error=""
    )
    if extra:
        state.update(extra)
    return state


def _fail_job(job_id: str, error: str) -> None:
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if job:
            job.status = "error"
            job.error_message = error
            db.commit()
    except SQLAlchemyError:
        db.rollback()
    finally:
        db.close()
    # Discard in-memory dataset on failure
    from app.utils.data_store import discard_dataset
    discard_dataset(job_id)


# ── Primary entry point (autonomous pipeline) ─────────────────────────────────

def run_autonomous_pipeline(job_id: str) -> None:
    """
    Execute the full autonomous analysis pipeline in one pass:
    Profile → Question → Plan → Execute → Insight → Report → Discard Data

    This is the UPDATED README workflow — no human approval gates.
    """
    import json
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job or job.status != "processing":
            logger.warning("Skipping pipeline for job %s (status=%s)", job_id, getattr(job, "status", "not found"))
            return
        initial_state = _build_initial_state(job)
    finally:
        db.close()

    try:
        final_state = unified_workflow.invoke(initial_state)
        final_error = final_state.get("error")
        report_path = final_state.get("report_path", "")

        if final_error:
            raise RuntimeError(str(final_error))
        if not report_path:
            raise RuntimeError("Workflow completed without generating a report")

        db = SessionLocal()
        try:
            job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
            if job and job.status != "error":
                job.status = "completed"
                job.result_path = report_path
                job.analysis_plan = json.dumps(final_state.get("analysis_plan", []))
                job.analytical_questions = json.dumps(final_state.get("analytical_questions", []))
                db.commit()
        except SQLAlchemyError as db_err:
            db.rollback()
            logger.error("Failed to finalize completed state for job %s: %s", job_id, db_err)
            raise
        finally:
            db.close()

    except Exception as e:
        logger.exception("Autonomous pipeline failed for job %s", job_id)
        _fail_job(job_id, str(e))



