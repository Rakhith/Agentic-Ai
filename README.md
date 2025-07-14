# 🤖 LangGraph Agentic AI for Sakila DB

A **reasoning-enabled, memory-capable agentic AI** that provides a conversational interface to the **Sakila MySQL database** and **profiling JSONs**, powered by **Gemini 1.5 Flash**, **LangChain**, and **LangGraph**.

---

## 🚀 Features

* 💬 Natural language interface to ask complex questions
* 🧠 Agent with **multi-turn memory and reasoning**
* 📊 Visual and tabular **data profiling summaries**
* 🧪 Auto-generates, validates, and executes **SQL queries**
* ♻️ Supports export to Excel and null-value plotting
* 🔍 Integrated schema validation with self-healing support (coming soon)

---

## 📁 Project Structure

```bash
database_profiling/
├── profiling_reports_json/         # JSON reports from data profiling tools
├── exports/                        # Excel and plot outputs
├── llm_query_interface.py          # Main conversational agent
├── profiling.py                    # (Optional) JSON profiling generator
├── sakila-schema.sql               # MySQL Sakila schema
├── sakila-data.sql                 # Sample Sakila data
└── README.md                       # This file
```

---

## 🧹 Dependencies

```bash
pip install langchain langgraph google-generativeai pandas matplotlib mysql-connector-python
```

> Tip: Use `--break-system-packages` on WSL or `--user` on shared systems if needed.

---

## 🔐 Environment Setup

Set your environment variable for the Google API key:

```bash
export GOOGLE_API_KEY="your-api-key-here"
```

And optionally MySQL credentials (or use defaults in code):

```bash
export MYSQL_USER="root"
export MYSQL_PASSWORD="yourpassword"
export MYSQL_HOST="localhost"
export MYSQL_DATABASE="sakila"
```

---

## 📄 Input Requirements

* **Profiling JSONs** should be placed in `./profiling_reports_json/`
* Each JSON should be named like `<tablename>_profile.json`
* Generated using tools like [ydata-profiling](https://github.com/ydataai/ydata-profiling)

---

## 🧠 Run the Agent

```bash
python3 llm_query_interface.py
```


## 💬 Sample Queries

**Data Exploration**

* "Show summary of all tables"
* "What columns have nulls?"
* "Plot null distribution"

**SQL/NL to SQL Conversion**

* "List the top 5 most rented movies by category"
* "Which actors have acted in more than 15 films?"
* "Find customers who haven’t rented anything in the last month"

**Complex Joins & Reasoning**

* "Which movies rented over 50 times feature actors in more than 10 films?"
* "Give customers who watched the longest duration of movies"

**Exporting**

* "Export profiling summary to Excel"
* "Plot columns with missing values"

---

## ✅ Design Highlights

* Uses `gemini-1.5-flash` via `ChatGoogleGenerativeAI`
* Auto-validates generated SQL against MySQL schema
* Uses `LangGraph` for agent logic, memory, and clean execution flow
* Designed to extend with vision, auto-repair, or DB writes

---

## ⚠️ Limitations

* SQL repair is partially implemented — full auto-correction coming soon
* Profiling is static — no live DB stats (unless extended)
* Assumes Sakila database structure for schema

---

## 👨‍💻 Author

**Rakshith Srinivasan**
