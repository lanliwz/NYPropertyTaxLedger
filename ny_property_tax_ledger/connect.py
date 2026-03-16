import os

from langchain_neo4j import Neo4jGraph
from langchain_openai import ChatOpenAI


llm = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o"), temperature=0)

graph = Neo4jGraph(
    url=os.getenv("Neo4jFinDBUrl"),
    username=os.getenv("Neo4jFinDBUserName"),
    password=os.getenv("Neo4jFinDBPassword"),
    database=os.getenv("NEO4J_TAX_DB_NAME", "taxjc"),
    enhanced_schema=True,
)
