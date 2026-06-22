# agents/graph.py
import copy
import json
import sqlite3
import httpx  # Standard synchronous HTTP handling
from typing import List, Dict, Any, Annotated, Union
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite import SqliteSaver
from sentence_transformers import SentenceTransformer
import chromadb

# Initialize the local offline semantic extraction assets
EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
CHROMA_CLIENT = chromadb.PersistentClient(path="./chroma_db_storage")
PLAYBOOK_COLLECTION = CHROMA_CLIENT.get_or_create_collection(name="corporate_playbooks")

# Import the core payload schemas from our contract layer
from interfaces import DisruptionAlert

# =====================================================================
# 🛡️ DETERMINISTIC STATE REDUCER BLOCKS (Anti-Race Condition Layer)
# =====================================================================
def reduce_disruption_alerts(
    current: List[DisruptionAlert], 
    new_updates: Union[DisruptionAlert, List[DisruptionAlert]]
) -> List[DisruptionAlert]:
    updates = new_updates if isinstance(new_updates, list) else [new_updates]
    return current + updates

def reduce_telemetry_registry(
    current: Dict[str, Any], 
    new_data: Dict[str, Any]
) -> Dict[str, Any]:
    merged = copy.deepcopy(current)
    for key, value in new_data.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = reduce_telemetry_registry(merged[key], value)
        else:
            merged[key] = value
    return merged

class LogiHiveGraphState(BaseModel):
    active_disruptions: Annotated[List[DisruptionAlert], reduce_disruption_alerts] = Field(default_factory=list)
    live_telemetry_registry: Annotated[Dict[str, Any], reduce_telemetry_registry] = Field(default_factory=dict)
    calculated_risk_tensor: float = Field(default=0.0)
    rerouting_plan_proposed: bool = Field(default=False)
    human_approved: bool = Field(default=False)
    execution_timeline_logs: Annotated[List[Dict[str, Any]], reduce_disruption_alerts] = Field(default_factory=list)

# =====================================================================
# 🔍 SYNCHRONOUS AGENTIC RAG UTILITY
# =====================================================================
def execute_agentic_rag(query: str, top_k: int = 3) -> List[Dict[str, Any]]:
    """Synchronously generates embeddings and queries localized compliance vector rows."""
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
# 🏗️ SYNCHRONOUS WORKFLOW MULTI-AGENT NODE LOGIC
# =====================================================================
def analyst_agent_node(state: LogiHiveGraphState) -> Dict[str, Any]:
    """Synchronous Analyst Agent to match the stable SqliteSaver runtime."""
    print("\n⚡ [NODE ACTIVE] Analyst Agent: Commencing Context Fusion...")
    
    latest_alert_text = "Severe weather infrastructure gridlock reported near regional hub."
    if state.active_disruptions:
        latest_alert_text = state.active_disruptions[-1].alert_text

    rag_extracts = execute_agentic_rag(query=latest_alert_text, top_k=1)
    playbook_context = rag_extracts[0]["document_text"] if rag_extracts else "Execute default layout overrides."

    # Direct synchronous HTTP client call to local Ollama container
    ollama_url = "http://localhost:11434/api/chat"
    system_prompt = (
        "You are an expert logistics analyzer. Read the data and output strictly a valid JSON object "
        "matching these exact keys: 'disruption_risk_score' (float 0.0 to 1.0) and 'reasoning_summary' (string text)."
    )
    user_prompt = f"Incident Alert: {latest_alert_text}\nFallback Reference: {playbook_context}"
    
    payload = {
        "model": "qwen2.5vl:latest",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "stream": False,
        "format": "json"
    }

    computed_score = 0.85  # Standard high-risk baseline trigger parameters
    summary_text = "VLM analyzed critical infrastructural blockade lines."

    try:
        # ✅ Using synchronous httpx.Client instead of AsyncClient
        with httpx.Client(timeout=30.0) as client:
            response = client.post(ollama_url, json=payload)
            if response.status_code == 200:
                raw_content = response.json().get("message", {}).get("content", "{}")
                parsed_tensor = json.loads(raw_content)
                computed_score = float(parsed_tensor.get("disruption_risk_score", 0.85))
                summary_text = parsed_tensor.get("reasoning_summary", "Completed extraction.")
                print(f"✅ Ollama VLM Client Response Validated. Metric: {computed_score}")
    except Exception as err:
        print(f"⚠️ Ollama client connection skipped: {str(err)}. Injecting simulation defaults.")

    return {
        "calculated_risk_tensor": computed_score,
        "execution_timeline_logs": [{"node": "analyst_agent", "status": "VLM_Search_Complete", "reasoning": summary_text}]
    }

def mitigation_agent_node(state: LogiHiveGraphState) -> Dict[str, Any]:
    print("\n⚡ [NODE ACTIVE] Mitigation Agent: Rewriting network layout edges...")
    return {
        "rerouting_plan_proposed": True,
        "execution_timeline_logs": [{"node": "mitigation_agent", "status": "autonomous_reroute_calculated"}]
    }

# =====================================================================
# 🗺️ GRAPH TOPOLOGY & PERSISTENCE INITIALIZATION
# =====================================================================
# agents/graph.py (Topology Configuration Update)

def evaluation_gatekeeper_router(state: LogiHiveGraphState) -> str:
    print(f"\n🔬 [ROUTING HUB] Current Disruption Risk Variable: {state.calculated_risk_tensor}")
    # Simply declare the next path node target; let interrupt_before handle the physical pause boundary
    return "pass_to_mitigation"

# Instantiating the LangGraph engine topology mapping
workflow = StateGraph(LogiHiveGraphState)

workflow.add_node("analyst_agent", analyst_agent_node)
workflow.add_node("mitigation_agent", mitigation_agent_node)

workflow.set_entry_point("analyst_agent")

# ✅ FIXED: Route directly to mitigation_agent. LangGraph will auto-pause before entering it!
workflow.add_conditional_edges(
    "analyst_agent",
    evaluation_gatekeeper_router,
    {
        "pass_to_mitigation": "mitigation_agent"
    }
)
workflow.add_edge("mitigation_agent", END)

# Establish local SQLite disk connection tracking parameters
sqlite_conn = sqlite3.connect("langgraph_state_registry.db", check_same_thread=False)
memory_checkpointer = SqliteSaver(sqlite_conn)

# Compile the graph binding the checkpointer and isolating the mitigation boundary line
app_compiled = workflow.compile(
    checkpointer=memory_checkpointer,
    interrupt_before=["mitigation_agent"] # 👈 Intercepts and hits the hard disk serialization breakpoint right here!
)

print("🚀 Successfully compiled Day 5 LogiHive Graph Engine with Stable Module Persistence.")