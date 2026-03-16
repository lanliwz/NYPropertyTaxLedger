import asyncio
import uuid

import streamlit as st
from langchain_core.messages import AIMessage, HumanMessage

from ny_property_tax_ledger.graph import tax_chatbot_graph


st.set_page_config(
    page_title="NY Property Tax Ledger",
    page_icon=":moneybag:",
    layout="wide",
)

st.markdown(
    """
<style>
    .stChatFloatingInputContainer {
        padding-bottom: 2rem;
    }
    .stChatMessage {
        border-radius: 15px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .main {
        background-color: #f8f9fa;
    }
    h1 {
        color: #1e3a8a;
    }
</style>
""",
    unsafe_allow_html=True,
)

if "messages" not in st.session_state:
    st.session_state.messages = []

if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

with st.sidebar:
    st.title("NY Property Tax Ledger")
    st.markdown("---")
    st.info("Ask about tax bills, payments, balances, and account history.")

    if st.button("Reset Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()

    st.markdown("---")
    st.subheader("Debug Info")
    st.write(f"Thread ID: `{st.session_state.thread_id}`")

st.title("Property Tax Billing and Payment Assistant")
st.caption("Powered by Neo4j and LangGraph")

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("cypher"):
            with st.expander("Show Generated Cypher"):
                st.code(message["cypher"], language="cypher")

if prompt := st.chat_input("Ask about property tax bills or payments..."):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        response_placeholder = st.empty()

        async def get_response():
            config = {"configurable": {"thread_id": st.session_state.thread_id}}
            final_content = ""
            final_cypher = ""

            async for event in tax_chatbot_graph.astream(
                {"messages": [HumanMessage(content=prompt)]},
                config,
                stream_mode="values",
            ):
                if "messages" in event:
                    last_msg = event["messages"][-1]
                    if isinstance(last_msg, AIMessage):
                        final_content = last_msg.content
                        response_placeholder.markdown(final_content)

                curr_state = await tax_chatbot_graph.aget_state(config)
                if curr_state.values.get("cypher_statement"):
                    final_cypher = curr_state.values["cypher_statement"]

            return final_content, final_cypher

        with st.spinner("Analyzing data..."):
            answer, cypher = asyncio.run(get_response())

        st.session_state.messages.append(
            {
                "role": "assistant",
                "content": answer,
                "cypher": cypher,
            }
        )

        if cypher:
            with st.expander("Show Generated Cypher"):
                st.code(cypher, language="cypher")
