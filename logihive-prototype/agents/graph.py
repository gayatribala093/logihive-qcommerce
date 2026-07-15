# agents/graph.py
import os
import sys
import httpx
from typing import List, Dict, Any
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# Ensure absolute system paths are resolved correctly for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from interfaces import DisruptionAlert

# LangGraph state contract definition
class CorporateHiveState(TypedDict, total=False):
    alert_context: DisruptionAlert
    risk_score: float
    history: List[str]
    plan_proposed: bool
    human_approved: bool

def analyst_agent(state: CorporateHiveState) -> Dict[str, Any]:
    print("⚡ [NODE ACTIVE] Analyst Agent: Commencing Context Fusion...")
    
    alert_context = state.get("alert_context")
    if hasattr(alert_context, "alert_text"):
        alert_text = alert_context.alert_text
    elif isinstance(alert_context, dict):
        alert_text = alert_context.get("alert_text", "")
    else:
        alert_text = "Standard regional operational loop pulse."
    
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "qwen2.5vl:latest",
                    "prompt": f"Analyze this supply chain incident report: '{alert_text}'. Return exactly a floating number between 0.0 and 1.0 representing disruption risk. Do not output anything else.",
                    "stream": False
                }
            )
            raw_output = response.json().get("response", "").strip()
            risk = float(''.join(c for c in raw_output if c.isdigit() or c == '.'))
            risk = max(0.0, min(1.0, risk))
    except Exception:
        # Failsafe fallback simulation value if local Ollama is offline
        risk = 0.85
        
    print(f"🔬 [ROUTING HUB] Current Disruption Risk Variable: {risk}")
    return {"risk_score": risk, "history": [f"Analyst evaluated risk at {risk}"]}

def evaluation_gatekeeper_router(state: CorporateHiveState) -> str:
    if state.get("risk_score", 0.0) > 0.75 and not state.get("human_approved", False):
        print("🚨 CRITICAL LINE: Safety threshold breached! Routing to Human Gatekeeper breakpoint.")
        return "trigger_human_gate"
    return "continue_to_mitigation"

def human_gate(state: CorporateHiveState) -> Dict[str, Any]:
    print("📥 [BREAKPOINT REACHED] Multi-agent state serialized. Awaiting supervisor token...")
    return {}

def mitigation_agent(state: CorporateHiveState) -> Dict[str, Any]:
    print("⚡ [NODE ACTIVE] Mitigation Agent: Rewriting network layout edges...")
    current_history = state.get("history", [])
    return {
        "plan_proposed": True, 
        "human_approved": True, 
        "history": current_history + ["Mitigation executed routing updates successfully."]
    }

# Workflow Architecture Mapping
def create_compiled_app():
    memory_checkpointer = MemorySaver()
    workflow = StateGraph(CorporateHiveState)

    workflow.add_node("analyst_agent", analyst_agent)
    workflow.add_node("human_gate", human_gate)
    workflow.add_node("mitigation_agent", mitigation_agent)

    workflow.set_entry_point("analyst_agent")

    workflow.add_conditional_edges(
        "analyst_agent",
        evaluation_gatekeeper_router,
        {
            "trigger_human_gate": "human_gate",
            "continue_to_mitigation": "mitigation_agent"
        }
    )
    workflow.add_edge("human_gate", "mitigation_agent")
    workflow.add_edge("mitigation_agent", END)

    return workflow.compile(
        checkpointer=memory_checkpointer,
        interrupt_before=["human_gate"]
    )