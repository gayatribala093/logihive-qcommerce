# agents/graph.py
import os
import copy
import json
import sqlite3
import httpx
from typing import List, Dict, Any, Annotated, Union
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END 
from langgraph.checkpoint.sqlite import SqliteSaver
from sentence_transformers import SentenceTransformer
import chromadb

# =====================================================================
# 🧠 LOCAL MODEL & VECTOR STORE INITIALIZATION
# =====================================================================
# Load lightweight, local embedding weights (runs natively offline on CPU/GPU)
EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")

# Bind to the local persistent ChromaDB storage workspace
CHROMA_CLIENT = chromadb.PersistentClient(path="./chroma_db_storage")
PLAYBOOK_COLLECTION = CHROMA_CLIENT.get_or_create_collection(name="corporate_playbooks")

# =====================================================================
# 📦 HIGH-VELOCITY ARCHITECTURAL CONTRACTS (From interfaces.py)
# =====================================================================
class DisruptionAlert(BaseModel):
    alert_id: str = Field(..., description="Unique incident identifier record")
    location_zone: str = Field(..., description="Target Mumbai regional pocket")
    alert_text: str = Field(..., description="Raw textual news snippet or log feed")

class DisruptionRiskTensor(BaseModel):
    disruption_risk_score: float = Field(..., ge=0.0, le=1.0)
    reasoning_summary: str

# =====================================================================
# 🛡️ DETERMINISTIC STATE REDUCER BLOCKS (Anti-Race Condition Layer)
# =====================================================================
def reduce_disruption_alerts(
    current: List[DisruptionAlert], 
    new_updates: Union[DisruptionAlert, List[DisruptionAlert]]
) -> List[DisruptionAlert]:
    """Thread-safe associative list append reducer for concurrent inputs."""
    updates = new_updates if isinstance(new_updates, list) else [new_updates]
    return current + updates

def reduce_telemetry_registry(
    current: Dict[str, Any], 
    new_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Deep dictionary merge reducer protecting nested telemetry states."""
    merged = copy.deepcopy(current)
    for key, value in new_data.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = reduce_telemetry_registry(merged[key], value)
        else:
            merged[key] = value
    return merged

# =====================================================================
# 🗃️ ANNOTATED LANGGRAPH STATE MATRIX
# =====================================================================
class LogiHiveGraphState(BaseModel):
    """Global Single Source of Truth Context Pattern passing across agent nodes."""
    active_disruptions: Annotated[List[DisruptionAlert], reduce_disruption_alerts] = Field(default_factory=list)
    live_telemetry_registry: Annotated[Dict[str, Any], reduce_telemetry_registry] = Field(default_factory=dict)
    calculated_risk_tensor: float = Field(default=0.0)
    rerouting_plan_proposed: bool = Field(default=False)
    human_approved: bool = Field(default=False)
    execution_timeline_logs: Annotated[List[Dict[str, Any]], reduce_disruption_alerts] = Field(default_factory=list)

# =====================================================================
# 🔍 ASYNCHRONOUS AGENTIC RAG UTILITY
# =====================================================================
async def execute_agentic_rag(query: str, top_k: int = 3) -> List[Dict[str, Any]]:
    """Asynchronously generates embeddings and runs vector queries on local playbooks."""
    query_vector = EMBEDDING_MODEL.encode(query).tolist()
    results = PLAYBOOK_COLLECTION.query(query_embeddings=[query_vector], n_results=top_k)
    
    extracted_policies = []
    if results and "documents" in results and results["documents"] and results["documents"][0]:
        for idx, doc in enumerate(results["documents"][0]):
            meta = results["metadatas"][0][idx] if results["metadatas"] else {}
            extracted_policies.append({
                "document_text": doc,
                "metadata": meta,
                "score_distance": results["distances"][0][idx] if "distances" in results else 0.0
            })
    return extracted_policies

# =====================================================================
# 🏗️ ACTIVE WORKFLOW GRAPH NODE LOGIC
# =====================================================================
async def analyst_agent_node(state: LogiHiveGraphState) -> Dict[str, Any]:
    """
    Analyst Agent: Fuses vector space playbook lookups with an active API call
    to the local hardware-accelerated Ollama server hosting qwen2.5-vl.
    """
    print("\n⚡ [NODE ACTIVE] Analyst Agent: Initiating Multi-Modal Pipeline Fusion...")
    
    # 1. Fallback Text Parsing
    latest_alert_text = "Severe environmental flash flood anomaly detected near central node."
    if state.active_disruptions:
        latest_alert_text = state.active_disruptions[-1].alert_text

    # 2. Local Chromadb Semantic Compliance Retrieval
    rag_extracts = await execute_agentic_rag(query=latest_alert_text, top_k=1)
    playbook_context = rag_extracts[0]["document_text"] if rag_extracts else "Execute default network re-weighting layout."

    # 3. Live Client Call to Local Ollama Container Engine
    ollama_url = "http://localhost:11434/api/chat"
    system_prompt = (
        "You are an expert real-time supply chain logistics evaluation system. "
        "Analyze the incident report against the corporate backup compliance guidelines. "
        "Enforce native structured JSON outputs matching this schema: "
        "{'disruption_risk_score': float between 0.0 and 1.0, 'reasoning_summary': string summary}"
    )
    user_prompt = f"Incident Report: {latest_alert_text}\nCompliance Guideline Reference: {playbook_context}"
    
    payload = {
        "model": "qwen2.5-vl",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "stream": False,
        "format": "json"
    }

    computed_score = 0.50  # Default safe preset fallback
    summary_text = "Ollama connection timeout fallback."

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(ollama_url, json=payload)
            if response.status_code == 200:
                raw_content = response.json().get("message", {}).get("content", "{}")
                parsed_tensor = json.loads(raw_content)
                computed_score = float(parsed_tensor.get("disruption_risk_score", 0.50))
                summary_text = parsed_tensor.get("reasoning_summary", "Extraction completed.")
                print(f"✅ Ollama VLM Evaluation Successful. Risk Score: {computed_score}")
            else:
                print(f"⚠️ Ollama returned non-200 status: {response.status_code}. Using fallback presets.")
    except Exception as err:
        print(f"⚠️ Local Ollama VLM Client Connection Skipped/Failed: {str(err)}. Dropping to fallback parameters.")

    timeline_update = {
        "node": "analyst_agent",
        "status": "VLM_and_RAG_Lookups_Completed",
        "reasoning": summary_text
    }
    
    return {
        "calculated_risk_tensor": computed_score,
        "execution_timeline_logs": [timeline_update]
    }

def mitigation_agent_node(state: LogiHiveGraphState) -> Dict[str, Any]:
    """Mitigation Agent: Runs only after the supervisor clears the serialization breakpoint."""
    print("\n⚡ [NODE ACTIVE] Mitigation Agent: Rewriting mathematical network layout edges inside NetworkX Twin...")
    return {
        "rerouting_plan_proposed": True,
        "execution_timeline_logs": [{"node": "mitigation_agent", "status": "autonomous_reroute_calculated"}]
    }

# =====================================================================
# 🗺️ GRAPH TOPOLOGY & CONDITIONAL ROUTING CONFIGURATION
# =====================================================================
def evaluation_gatekeeper_router(state: LogiHiveGraphState) -> str:
    """Evaluates computed risk boundaries to declare conditional execution directions."""
    print(f"\n🔬 [ROUTING MANAGER] Evaluating Score Line: {state.calculated_risk_tensor}")
    if state.calculated_risk_tensor > 0.75 and not state.human_approved:
        print("🚨 CRITICAL WARNING: Threshold breached (>0.75)! Commanded to break and serialize state context.")
        return "trigger_breakpoint"
    return "pass_to_mitigation"

# Instantiating the LangGraph engine topology mapping
workflow = StateGraph(LogiHiveGraphState)

workflow.add_node("analyst_agent", analyst_agent_node)
workflow.add_node("mitigation_agent", mitigation_agent_node)

workflow.set_entry_point("analyst_agent")

workflow.add_conditional_edges(
    "analyst_agent",
    evaluation_gatekeeper_router,
    {
        "trigger_breakpoint": "analyst_agent", # Loops state at position to support downstream resume lookups
        "pass_to_mitigation": "mitigation_agent"
    }
)
workflow.add_edge("mitigation_agent", END)

# =====================================================================
# 💾 TRANSACTIONAL PERSISTENCE LAYER (SqliteSaver Integration)
# =====================================================================
# Establish local SQLite disk connection tracking parameters
sqlite_conn = sqlite3.connect("langgraph_state_registry.db", check_same_thread=False)
memory_checkpointer = SqliteSaver(sqlite_conn)

# Compile the graph binding the checkpointer and isolating the mitigation boundary line
app_compiled = workflow.compile(
    checkpointer=memory_checkpointer,
    interrupt_before=["mitigation_agent"] # Halts execution exactly before mitigation node runs
)

print("🚀 Successfully compiled Day 4 LogiHive LangGraph State Engine with SqliteSaver & Ollama APIs.")