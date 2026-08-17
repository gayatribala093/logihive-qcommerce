"""
agents/graph.py
LogiHive Q-Commerce :: Multi-Agent Resilience Engine
-----------------------------------------------------
LangGraph state machine orchestrating the agentic digital twin's
disruption-response pipeline: ingest -> RAG-augmented analysis ->
risk scoring -> conditional human-in-the-loop gate -> mitigation dispatch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import operator
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Any, Literal, TypedDict

import httpx
import chromadb
from chromadb.utils import embedding_functions
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver

from interfaces import DisruptionAlert  # noqa: F401  (kept for downstream typing use)

logger = logging.getLogger("logihive.agents.graph")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

OLLAMA_HOST = "http://localhost:11434"
OLLAMA_VLM_MODEL = "qwen2.5-vl"
OLLAMA_TIMEOUT_S = 60.0
OLLAMA_KEEP_ALIVE = "30m"  # keep the model resident between requests instead of reloading each time
FALLBACK_RISK_SCORE = 0.85
HUMAN_GATE_RISK_THRESHOLD = 0.75
CHROMA_PERSIST_DIR = "./chroma_sop_store"
CHROMA_COLLECTION = "sop_playbooks"
SQLITE_CHECKPOINT_PATH = "langgraph_state_registry.db"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"


# --------------------------------------------------------------------------
# State Schema
# --------------------------------------------------------------------------

class AgentState(TypedDict):
    """Shared graph state. List fields use additive reducers so that
    sequential/parallel node writes accumulate rather than overwrite,
    which matters here because multiple nodes append to the same
    audit trail across a single run (and across resumed checkpoints).
    """

    raw_alert_text: str
    alert: DisruptionAlert | None
    retrieved_sops: Annotated[list[str], operator.add]
    agent_trace: Annotated[list[str], operator.add]
    disruption_risk_score: float
    risk_tensor: dict[str, Any]
    requires_human_review: bool
    human_decision: str | None
    mitigation_actions: Annotated[list[str], operator.add]


# --------------------------------------------------------------------------
# Chroma / RAG setup (lazy singletons so import stays cheap in tests)
# --------------------------------------------------------------------------

_chroma_client: chromadb.ClientAPI | None = None
_sop_collection = None


def _get_sop_collection():
    """Lazily initialize (or fetch) the persistent Chroma collection that
    stores embedded SOP playbook excerpts. Deferred to first use so
    importing this module never touches disk / spins up a model.
    """
    global _chroma_client, _sop_collection
    if _sop_collection is not None:
        return _sop_collection

    _chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
    embed_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=EMBEDDING_MODEL_NAME
    )
    _sop_collection = _chroma_client.get_or_create_collection(
        name=CHROMA_COLLECTION,
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine"},
    )
    return _sop_collection


def execute_agentic_rag(query: str, top_k: int = 3) -> list[str]:
    """Query the local SOP playbook vector store (ChromaDB + sentence-
    transformers embeddings) for the most relevant standard-operating-
    procedure excerpts given a disruption description.

    This is a synchronous helper by design: ChromaDB's local client is
    not async, and LangGraph node functions are expected to be short,
    blocking units of work executed by the graph's own runner.
    """
    if not query or not query.strip():
        return []

    try:
        collection = _get_sop_collection()
        results = collection.query(query_texts=[query], n_results=top_k)
        documents = results.get("documents", [[]])[0]
        if not documents:
            logger.info("Agentic RAG: no SOP matches found for query.")
            return ["No matching SOP found. Defer to on-call ops manager."]
        return documents
    except Exception as exc:  # noqa: BLE001 - vector store failures must not crash the graph
        logger.warning("Agentic RAG lookup failed: %s", exc)
        return ["SOP retrieval unavailable - fallback to manual triage."]


# --------------------------------------------------------------------------
# Ollama VLM risk-parsing helper
# --------------------------------------------------------------------------

_RISK_PARSE_PROMPT = """You are a supply-chain disruption risk analyst.
Read the alert text and output ONLY a minified JSON object (no prose, no
markdown fences) with this exact schema:
{{"disruption_risk_score": <float 0.0-1.0>, "category": "<string>", "confidence": <float 0.0-1.0>}}

Alert text:
{alert_text}
"""


def _call_ollama_vlm(alert_text: str) -> dict[str, Any]:
    """Synchronous call to a local Ollama server running qwen2.5-vl.

    Returns a parsed risk tensor dict of the form:
        {"disruption_risk_score": float, "category": str,
         "confidence": float, "source": "ollama_vlm" | "fallback"}

    Falls back to a conservative fixed score (FALLBACK_RISK_SCORE) if
    Ollama is unreachable, times out, or returns malformed JSON - the
    analyst node must never hard-fail the graph run on an infra blip.
    """
    payload = {
        "model": OLLAMA_VLM_MODEL,
        "prompt": _RISK_PARSE_PROMPT.format(alert_text=alert_text),
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1},
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }

    try:
        with httpx.Client(timeout=OLLAMA_TIMEOUT_S) as client:
            resp = client.post(f"{OLLAMA_HOST}/api/generate", json=payload)
            resp.raise_for_status()
            body = resp.json()
            raw_content = body.get("response", "").strip()
            tensor = json.loads(raw_content)

            score = float(tensor.get("disruption_risk_score", FALLBACK_RISK_SCORE))
            score = max(0.0, min(1.0, score))
            tensor["disruption_risk_score"] = score
            tensor.setdefault("category", "unclassified")
            tensor.setdefault("confidence", 0.5)
            tensor["source"] = "ollama_vlm"
            return tensor

    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        logger.warning("Ollama VLM unreachable (%s) - using fallback risk score.", exc)
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.warning("Ollama VLM returned malformed output (%s) - using fallback.", exc)
    except Exception as exc:  # noqa: BLE001 - never let a model hiccup kill the pipeline
        logger.error("Unexpected Ollama VLM failure: %s", exc)

    return {
        "disruption_risk_score": FALLBACK_RISK_SCORE,
        "category": "unclassified",
        "confidence": 0.0,
        "source": "fallback",
    }


# --------------------------------------------------------------------------
# Graph Nodes
# --------------------------------------------------------------------------

def ingest_node(state: AgentState) -> dict[str, Any]:
    """Normalize raw alert text as it enters the pipeline."""
    text = state["raw_alert_text"]
    logger.info("Ingesting alert: %.80s", text)
    return {
        "agent_trace": [f"ingest_node: received {len(text)} chars of alert text"],
    }


def rag_context_node(state: AgentState) -> dict[str, Any]:
    """Analyst-support node: pull relevant SOP playbook excerpts via
    execute_agentic_rag so the Analyst Agent has grounded context
    before it scores risk.
    """
    sops = execute_agentic_rag(state["raw_alert_text"], top_k=3)
    return {
        "retrieved_sops": sops,
        "agent_trace": [f"rag_context_node: retrieved {len(sops)} SOP excerpt(s)"],
    }


def analyst_agent_node(state: AgentState) -> dict[str, Any]:
    """Core Analyst Agent: parses unstructured alert text via a local
    Ollama VLM (qwen2.5-vl) into a minified JSON risk tensor. Falls back
    to a fixed conservative score (0.85) if Ollama is unreachable, and
    flags the run for human review whenever risk exceeds the configured
    threshold.
    """
    risk_tensor = _call_ollama_vlm(state["raw_alert_text"])
    score = risk_tensor["disruption_risk_score"]
    needs_review = score > HUMAN_GATE_RISK_THRESHOLD

    logger.info(
        "Analyst agent scored risk=%.2f (human_review=%s)", score, needs_review
    )

    return {
        "disruption_risk_score": score,
        "risk_tensor": risk_tensor,
        "requires_human_review": needs_review,
        "agent_trace": [
            f"analyst_agent_node: risk_score={score:.2f} "
            f"category={risk_tensor.get('category')} source={risk_tensor.get('source')}"
        ],
    }


def human_gate_node(state: AgentState) -> dict[str, Any]:
    """HITL breakpoint. The compiled graph is built with
    interrupt_before=["human_gate"], so LangGraph pauses execution
    *before* this node runs whenever the analyst routes here. A human
    operator (via the FastAPI control endpoint) is expected to call
    graph.update_state(config, {"human_decision": "APPROVE" | "REJECT"})
    and then resume the run. This node body simply records the decision
    that was injected into state.
    """
    decision = state.get("human_decision") or "PENDING_APPROVAL"
    logger.info("human_gate_node: decision=%s", decision)
    return {
        "agent_trace": [f"human_gate_node: operator decision = {decision}"],
    }


def mitigation_dispatch_node(state: AgentState) -> dict[str, Any]:
    """Dispatch mitigation actions based on the risk tensor and, when
    applicable, the human sign-off recorded at the HITL gate. This is
    the hand-off point to downstream systems (rerouting engine, dark
    store rebalancer, ops control tower notifications).
    """
    score = state["disruption_risk_score"]
    decision = state.get("human_decision")

    if state.get("requires_human_review") and decision not in ("APPROVE", "APPROVED"):
        actions = ["HOLD: awaiting human approval before dispatch"]
    elif score >= 0.5:
        actions = [
            "reroute_affected_riders",
            "rebalance_dark_store_inventory",
            "notify_ops_control_tower",
        ]
    else:
        actions = ["log_and_monitor"]

    return {
        "mitigation_actions": actions,
        "agent_trace": [f"mitigation_dispatch_node: dispatched {actions}"],
    }


# --------------------------------------------------------------------------
# Conditional routing
# --------------------------------------------------------------------------

def route_after_analysis(state: AgentState) -> Literal["human_gate", "mitigation_dispatch"]:
    """Send high-risk alerts through the HITL gate; everything else
    goes straight to mitigation dispatch.
    """
    if state.get("requires_human_review"):
        return "human_gate"
    return "mitigation_dispatch"


# --------------------------------------------------------------------------
# Graph assembly
# --------------------------------------------------------------------------

def build_graph():
    """Compile the LangGraph state machine with SQLite-backed persistent
    checkpointing (langgraph_state_registry.db) and a HITL interrupt
    immediately before the human_gate node.
    """
    workflow = StateGraph(AgentState)

    workflow.add_node("ingest", ingest_node)
    workflow.add_node("rag_context", rag_context_node)
    workflow.add_node("analyst_agent", analyst_agent_node)
    workflow.add_node("human_gate", human_gate_node)
    workflow.add_node("mitigation_dispatch", mitigation_dispatch_node)

    workflow.add_edge(START, "ingest")
    workflow.add_edge("ingest", "rag_context")
    workflow.add_edge("rag_context", "analyst_agent")

    workflow.add_conditional_edges(
        "analyst_agent",
        route_after_analysis,
        {
            "human_gate": "human_gate",
            "mitigation_dispatch": "mitigation_dispatch",
        },
    )

    workflow.add_edge("human_gate", "mitigation_dispatch")
    workflow.add_edge("mitigation_dispatch", END)

    # SqliteSaver.from_conn_string(...) returns a *generator-based* context
    # manager. Calling only __enter__() on it and discarding the generator
    # itself is a trap: once the generator object is garbage-collected
    # (which happens as soon as this function returns, since nothing else
    # references it), Python runs its `finally:` block, which closes the
    # underlying sqlite3 connection out from under the live graph -
    # surfacing later as "ProgrammingError: Cannot operate on a closed
    # database." Constructing the Connection ourselves and handing it to
    # SqliteSaver directly avoids that lifecycle entirely: the connection
    # is kept alive for as long as the checkpointer (and therefore the
    # compiled graph singleton) is alive.
    conn = sqlite3.connect(SQLITE_CHECKPOINT_PATH, check_same_thread=False)
    checkpointer = SqliteSaver(conn)

    compiled = workflow.compile(
        checkpointer=checkpointer,
        interrupt_before=["human_gate"],
    )
    return compiled


# Module-level compiled graph singleton, built lazily to avoid import-time
# side effects (SQLite file creation, Chroma client init) during unit tests.
_compiled_graph = None


def get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_graph()
    return _compiled_graph


# --------------------------------------------------------------------------
# Async wrappers for the FastAPI layer
# --------------------------------------------------------------------------
# graph.invoke / graph.update_state are blocking calls that go through the
# SqliteSaver checkpointer. Sqlite connections created with the default
# check_same_thread=True are not safe to hit from an arbitrary thread pool,
# so all graph execution is serialized onto a single dedicated worker
# thread rather than the shared CPU-bound EXECUTOR in interfaces.py (which
# is reserved for NetworkX/Dijkstra work in core/twin.py). This keeps every
# checkpoint read/write on one thread while still keeping the FastAPI event
# loop unblocked.

_graph_run_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="langgraph-runner")


async def ainvoke_graph(input_state: dict[str, Any], config: dict[str, Any]) -> AgentState:
    """Run the graph from START, non-blocking from the caller's perspective.
    If the run hits the human_gate interrupt, this returns the state as of
    the pause point rather than raising.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _graph_run_executor, lambda: get_graph().invoke(input_state, config)
    )


async def aresume_graph(config: dict[str, Any], state_update: dict[str, Any]) -> AgentState:
    """Inject a state update (typically {'human_decision': 'APPROVE'|'REJECT'})
    at a paused checkpoint and resume execution to completion or the next
    interrupt.
    """
    loop = asyncio.get_running_loop()

    def _run() -> AgentState:
        graph = get_graph()
        graph.update_state(config, state_update)
        return graph.invoke(None, config)

    return await loop.run_in_executor(_graph_run_executor, _run)


async def aget_state(config: dict[str, Any]):
    """Fetch the current checkpointed state (and pending-node info) for a
    given thread without mutating anything.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_graph_run_executor, lambda: get_graph().get_state(config))


__all__ = [
    "AgentState",
    "execute_agentic_rag",
    "build_graph",
    "get_graph",
    "ainvoke_graph",
    "aresume_graph",
    "aget_state",
    "HUMAN_GATE_RISK_THRESHOLD",
]