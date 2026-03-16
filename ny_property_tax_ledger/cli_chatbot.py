import asyncio
import uuid

from langchain_core.messages import AIMessage, HumanMessage

from ny_property_tax_ledger.graph import tax_chatbot_graph


async def run_test_conversation():
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    questions = [
        "What is the total billed amount in 2025?",
        "Show billed and paid totals by account for 2025.",
    ]

    for question in questions:
        print(f"\nUser: {question}")
        async for event in tax_chatbot_graph.astream(
            {"messages": [HumanMessage(content=question)]},
            config,
            stream_mode="values",
        ):
            if "messages" in event:
                last_msg = event["messages"][-1]
                if isinstance(last_msg, AIMessage):
                    pass

        final_state = await tax_chatbot_graph.aget_state(config)
        print(f"AI: {final_state.values['messages'][-1].content}")
        if final_state.values.get("cypher_statement"):
            print(f"Generated Cypher: {final_state.values['cypher_statement']}")


if __name__ == "__main__":
    asyncio.run(run_test_conversation())
