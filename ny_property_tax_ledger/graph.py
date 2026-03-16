from operator import add
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from ny_property_tax_ledger.connect import graph, llm
from ny_property_tax_ledger.query_examples import examples


class TaxChatState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    cypher_statement: Optional[str]
    cypher_errors: Annotated[List[str], add]
    database_records: Optional[Union[str, List[Dict[str, Any]]]]
    next_action: Optional[str]


class GuardDecision(BaseModel):
    decision: Literal["continue", "end"] = Field(
        description="Whether to continue the conversation or end it because the topic is unrelated."
    )


async def guard_node(state: TaxChatState):
    system_msg = """
    You are an assistant for New York property tax ledger questions.
    Decide if the user's request is related to tax, billing, payment, balance, or account history.
    If it is related, output "continue". Otherwise, output "end".
    """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_msg),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )

    chain = prompt | llm.with_structured_output(GuardDecision, method="json_schema")
    result = await chain.ainvoke({"messages": state["messages"]})

    if result.decision == "end":
        return {
            "messages": [
                AIMessage(
                    content="I can help with property tax bills, payments, balances, and account history."
                )
            ],
            "next_action": "end",
        }

    return {"next_action": "continue"}


async def generate_cypher_node(state: TaxChatState):
    fewshot_text = "\n\n".join(
        [f"Question: {example['question']}\nCypher: {example['query']}" for example in examples]
    )
    schema_escaped = graph.schema.replace("{", "{{").replace("}", "}}")
    fewshot_escaped = fewshot_text.replace("{", "{{").replace("}", "}}")

    system_msg = f"""
    You are a Neo4j expert working with a property tax ledger.
    Convert the user's request into a valid Cypher query for the following schema:
    {schema_escaped}

    Use these examples for guidance:
    {fewshot_escaped}

    Follow these rules:
    - Use TaxBilling for billed amounts.
    - Use TaxPayment for paid amounts.
    - Payments may be stored as negative values, so use abs(sum(p.Paid)) when returning a human-facing total.
    - Return only Cypher with no markdown fences.
    """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_msg),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )

    chain = prompt | llm | StrOutputParser()
    raw_cypher = await chain.ainvoke({"messages": state["messages"]})
    cypher = raw_cypher.replace("```cypher", "").replace("```", "").strip()
    return {"cypher_statement": cypher}


async def execute_query_node(state: TaxChatState):
    cypher = state.get("cypher_statement")
    if not cypher:
        return {"database_records": "No query generated."}

    try:
        records = graph.query(cypher)
        return {"database_records": records if records else "No records found."}
    except Exception as exc:
        return {
            "cypher_errors": [str(exc)],
            "database_records": f"Error executing query: {exc}",
        }


async def final_answer_node(state: TaxChatState):
    results = state.get("database_records")

    system_msg = """
    You are a helpful property tax ledger assistant.
    Answer clearly using the database results.
    If no results were found, explain that politely.
    """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_msg),
            MessagesPlaceholder(variable_name="messages"),
            ("human", "Database results: {results}"),
        ]
    )

    chain = prompt | llm | StrOutputParser()
    answer = await chain.ainvoke({"messages": state["messages"], "results": str(results)})
    return {"messages": [AIMessage(content=answer)]}


def should_continue(state: TaxChatState):
    if state.get("next_action") == "end":
        return END
    return "generate_cypher"


builder = StateGraph(TaxChatState)
builder.add_node("guard", guard_node)
builder.add_node("generate_cypher", generate_cypher_node)
builder.add_node("execute_query", execute_query_node)
builder.add_node("final_answer", final_answer_node)

builder.add_edge(START, "guard")
builder.add_conditional_edges("guard", should_continue)
builder.add_edge("generate_cypher", "execute_query")
builder.add_edge("execute_query", "final_answer")
builder.add_edge("final_answer", END)

memory = InMemorySaver()
tax_chatbot_graph = builder.compile(checkpointer=memory)
