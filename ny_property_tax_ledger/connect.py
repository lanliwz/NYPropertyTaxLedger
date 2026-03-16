import os

from langchain.chat_models import init_chat_model
from langchain_neo4j import Neo4jGraph


# LangChain v1 recommends initializing chat models through init_chat_model.
llm = init_chat_model(
    model=os.getenv("OPENAI_MODEL", "openai:gpt-4o"),
    temperature=0,
)

graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USERNAME"),
    password=os.getenv("NEO4J_PASSWORD"),
    database=os.getenv("NEO4J_TAX_DB_NAME", "tax62n"),
    enhanced_schema=True,
)
