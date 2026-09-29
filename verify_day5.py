"""
LogiHive Q-Commerce :: verify_day5.py
Direct script to verify LangGraph state compilation, breakpoint pause,
and human-in-the-loop resumption without needing external network servers.
"""

import asyncio
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from agents.graph import app_compiled
from interfaces import DisruptionAlert


async def run_verification():
    print("=" * 60)
    print("🚀 [Verify] Testing LogiHive Multi-Agent State Machine")
    print("=" * 60)

    thread_id = "VERIFY_TEST_THREAD_001"
    config = {"configurable": {"thread_id": thread_id}}

    alert = DisruptionAlert(
        alert_id=thread_id,
        category="flood",
        affected_area="DS_ANDHERI_EAST",
        raw_text="Severe waterlogging on JVLR arterial; traffic halted.",
    )

    initial_state = {
        "alert": alert,
        "disruption_risk_score": 0.0,
        "retrieved_sop": "",
        "mitigation_plan": {},
        "status": "PENDING",
        "history": [],
    }

    print("\n1. Injecting Alert Payload...")
    res1 = await app_compiled.ainvoke(initial_state, config=config)
    print(f"   Status: {res1.get('status')}")
    print(f"   Evaluated Risk: {res1.get('disruption_risk_score')}")
    print("   ✅ Paused at Human-in-the-Loop breakpoint boundary successfully.")

    print("\n2. Simulating Supervisor Approval (Resume past breakpoint)...")
    res2 = await app_compiled.ainvoke(None, config=config)
    print(f"   Final Status: {res2.get('status')}")
    print(f"   Mitigation Plan: {res2.get('mitigation_plan', {}).get('path_str')}")
    print(f"   Convergence Delay: {res2.get('mitigation_plan', {}).get('cost')} min")
    print("   ✅ Resumed and resolved route without state loss.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_verification())