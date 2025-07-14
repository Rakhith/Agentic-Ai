import os
import json
import logging
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import sqlparse
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import ChatPromptTemplate

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)

# === Configuration ===
PROFILE_DIR = "./profiling_reports_json"
EXPORT_DIR = "./exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# === Load All Profiles ===
profiles = {}
try:
    for filename in os.listdir(PROFILE_DIR):
        if filename.endswith(".json"):
            table_name = filename.split("_")[0]
            with open(os.path.join(PROFILE_DIR, filename), "r") as f:
                profiles[table_name] = json.load(f)
except Exception as e:
    logging.error(f"Error loading profiling files: {e}")
    exit()

if not profiles:
    logging.warning("No profiling JSON files found.")
    exit()

# === Summary Builder ===
def summarize_profile(profile, table_name):
    lines = []
    vars_info = profile.get("variables", {})
    n_rows = profile.get("table", {}).get("n", "?")
    lines.append(f"\n📄 Table: `{table_name}`")
    lines.append(f"- Rows: {n_rows}")
    lines.append("- Columns:")
    for col, info in vars_info.items():
        dtype = info.get("type", "unknown")
        nulls = info.get("n_missing", 0)
        uniq = info.get("n_unique", "?")
        lines.append(f"  - {col}: {dtype} (nulls: {nulls}, unique: {uniq})")
    return "\n".join(lines)

all_summaries = "\n".join(
    summarize_profile(profile, name) for name, profile in profiles.items()
)

# === Schema Extraction ===
def get_mysql_schema():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "21102004"),
        database=os.getenv("MYSQL_DATABASE", "sakila")
    )
    cursor = connection.cursor()
    cursor.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'sakila'")
    schema = {}
    for table, column in cursor.fetchall():
        schema.setdefault(table, []).append(column)
    cursor.close()
    connection.close()
    return schema

# === Tools ===
def view_summary_tool():
    return all_summaries

def get_null_columns():
    result = []
    for table_name, profile in profiles.items():
        vars_info = profile.get("variables", {})
        for col, info in vars_info.items():
            nulls = info.get("n_missing", 0)
            if nulls > 0:
                result.append(f"{table_name}.{col}: {nulls} nulls")
    return "\n".join(result) or "No nulls found."

def export_to_excel():
    for table_name, profile in profiles.items():
        df_data = []
        for col, info in profile.get("variables", {}).items():
            df_data.append({
                "Column": col,
                "Type": info.get("type", "unknown"),
                "Nulls": info.get("n_missing", 0),
                "Unique": info.get("n_unique", "?")
            })
        df = pd.DataFrame(df_data)
        df.to_excel(f"{EXPORT_DIR}/{table_name}_summary.xlsx", index=False)
    return f"Excel files exported to '{EXPORT_DIR}'"

def plot_nulls():
    data = []
    for table_name, profile in profiles.items():
        for col, info in profile.get("variables", {}).items():
            nulls = info.get("n_missing", 0)
            if nulls > 0:
                data.append((f"{table_name}.{col}", nulls))
    if not data:
        return "No nulls to plot."
    labels, values = zip(*data)
    plt.figure(figsize=(10, 6))
    plt.barh(labels, values)
    plt.xlabel("Null Count")
    plt.title("Columns with Null Values")
    plt.tight_layout()
    plot_path = f"{EXPORT_DIR}/nulls_plot.png"
    plt.savefig(plot_path)
    plt.close()
    return f"Plot saved to {plot_path}"

def execute_mysql_query(query):
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "21102004"),
            database=os.getenv("MYSQL_DATABASE", "sakila")
        )
        cursor = connection.cursor()
        cursor.execute(query)
        if cursor.with_rows:
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()
            formatted = [dict(zip(columns, row)) for row in result]
        else:
            connection.commit()
            formatted = f"Query executed successfully. Rows affected: {cursor.rowcount}"
        cursor.close()
        connection.close()
        return formatted
    except mysql.connector.Error as err:
        return f"MySQL Error: {err}"

def validate_sql_schema(sql, schema):
    invalid_references = []
    tables = set(schema.keys())
    columns = {f"{t}.{c}" for t, cols in schema.items() for c in cols}

    parsed = sqlparse.parse(sql)
    tokens = [str(t) for stmt in parsed for t in stmt.flatten() if t.ttype is None]

    for token in tokens:
        token = token.strip(" ,();")
        if "." in token:
            tbl, col = token.split(".", 1)
            if f"{tbl}.{col}" not in columns:
                invalid_references.append(token)
        elif token.lower() in tables:
            continue
        elif token.isidentifier() and token.lower() not in {
            "select", "from", "where", "join", "on", "and", "or",
            "group", "by", "having", "order", "limit", "as", "in", "exists"
        }:
            continue
    return list(set(invalid_references))

# === Define Tools ===
tools = {
    "summary": view_summary_tool,
    "nulls": get_null_columns,
    "export": export_to_excel,
    "plot": plot_nulls,
    "sql": execute_mysql_query,
}

# === Initialize LLM ===
api_key = os.getenv("GOOGLE_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key
llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash")
schema = get_mysql_schema()
schema_text = "\n".join(f"{table}: {', '.join(cols)}" for table, cols in schema.items())

# === LangGraph Nodes ===
def decide_action(state):
    query = state.get("input", "").strip()
    memory = state.get("memory", [])
    memory.append(query)

    try:
        if not query or query.lower() in ["hi", "hello"]:
            return {"output": "👋 Hi! How can I assist you with the Sakila database today?", "memory": memory}

        if "summary" in query:
            return {"output": tools["summary"](), "memory": memory}
        elif "null" in query and "plot" in query:
            return {"output": tools["plot"](), "memory": memory}
        elif "null" in query:
            return {"output": tools["nulls"](), "memory": memory}
        elif "excel" in query or "export" in query:
            return {"output": tools["export"](), "memory": memory}

        if query.strip().lower().startswith(("select", "update", "delete", "insert", "create", "drop")):
            return {"output": tools["sql"](query), "memory": memory}

        conversation_context = "\n".join(f"Q: {q}" for q in memory[-5:])
        prompt = f"""You are an SQL expert. Use the following schema:

{schema_text}

Based on the following recent conversation:
{conversation_context}

Translate the user's latest query into a valid MySQL query only (no explanation):
{query}
SQL:"""

        response = llm.invoke(prompt)
        sql_query = response.content.strip()

        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]

        sql_query = sql_query.strip()
        print("📝 Cleaned SQL:\n", sql_query)

        invalids = validate_sql_schema(sql_query, schema)
        if invalids:
            return {"output": f"❌ Invalid SQL references: {invalids}", "memory": memory}

        if sql_query.lower().startswith(("select", "update", "delete", "insert", "create", "drop")):
            return {"output": tools["sql"](sql_query), "memory": memory}
        else:
            return {"output": f"⚠️ Could not parse a valid SQL query.\nResponse: {sql_query}", "memory": memory}

    except Exception as e:
        return {"output": f"❌ Error: {str(e)}", "memory": memory}

# === LangGraph Build ===
builder = StateGraph(dict)
builder.add_node("AgentLogic", RunnableLambda(decide_action))
builder.set_entry_point("AgentLogic")
builder.add_edge("AgentLogic", END)

graph = builder.compile()

# === CLI Loop ===
print("\n🧠 LangGraph Agent Ready!")
print("Ask things like: 'Show summary',  'Plot nulls', or ask any query about the sakila database in natural language. Type 'exit' to quit.")

conversation_memory = []
while True:
    user_input = input("\n🤖 Ask: ")
    if user_input.lower() in ["exit", "quit"]:
        print("👋 Goodbye!")
        break
    result = graph.invoke({"input": user_input, "memory": conversation_memory})
    conversation_memory = result.get("memory", conversation_memory)
    if result and "output" in result:
        print("\n💡 Response:\n", result["output"])
    else:
        print("⚠️ No output returned.")
