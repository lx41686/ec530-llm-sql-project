# Building Data Systems with LLM Interfaces

This project implements a modular data system that allows users to:

- Load structured data from CSV files into a SQLite database
- Query the database using SQL or natural language
- Translate natural language into SQL using an LLM
- Validate generated SQL before execution to ensure safety

## 🧠 System Overview

The system follows a modular architecture with strict separation of concerns.

<img width="1339" height="760" alt="Screenshot 2026-04-09 at 23 05 02" src="https://github.com/user-attachments/assets/17ffe79e-220f-4f73-a14b-a29aad6ef0ce" />


### Main Components

- **CLI Interface**
  - Handles user interaction
  - Does NOT access the database directly

- **Query Service**
  - Orchestrates query flow
  - Connects LLM, validator, and database

- **LLM Adapter**
  - Converts natural language → SQL
  - Uses OpenAI API
  - Treated as an untrusted component

- **SQL Validator**
  - Ensures safety before execution
  - Rejects invalid or unsafe queries

- **Schema Manager**
  - Understands database structure
  - Provides schema context to LLM and validator

- **Data Loader**
  - Loads CSV into database
  - Creates tables dynamically

- **Database Manager**
  - Executes SQL queries
  - Manages SQLite connection

---

## 🔄 System Architecture

User
↓
CLI Interface
↓
Query Service
↓
LLM Adapter → (generates SQL)
↓
SQL Validator → (checks safety)
↓
Database (SQLite)
↓
Results returned to CLI

## ⚙️ Data Flow

### 1. Data Ingestion

CSV → Data Loader → Schema Manager → Database

- CSV is read using pandas
- Schema is inferred
- Table is created manually (no `df.to_sql`)
- Data is inserted row-by-row

### 2. Query Processing

User → CLI → Query Service → LLM → Validator → DB

- Natural language is converted to SQL
- SQL is validated before execution
- Only safe queries reach the database

## 🔐 SQL Validator Design

The validator is a critical safety component.

It enforces the following rules:

- Only `SELECT` queries are allowed
- Only a single statement is allowed
- Rejects unknown tables
- Rejects unknown columns

### Example

SELECT name FROM users;        ✅ Allowed

DELETE FROM users;             ❌ Rejected

SELECT salary FROM users;      ❌ Rejected (unknown column)

SELECT * FROM orders;          ❌ Rejected (unknown table)

## 🤖 LLM Integration

The system uses OpenAI API to translate natural language into SQL.

### Important Design Principle

> LLM output is treated as untrusted input.

Even if the LLM generates incorrect or unsafe SQL, the validator prevents execution.

## 🧪 Testing Strategy

The project includes unit tests for all core modules:

- Data Loader
- Schema Manager
- SQL Validator
- Query Service
- LLM Adapter (mocked)

### Key Design Choices

- Tests do NOT depend on real API calls
- Fake LLM clients are used for testing
- Each module is tested independently

## 🚀 How to Run

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Set up environment variables

Create a `.env` file:

```
OPENAI_API_KEY=your_api_key
OPENAI_MODEL=gpt-5.2
```

### 3. Run the system

```python
python -m src.main
```

### 4. Example Usage

1. Load CSV
2. Run SQL query
3. Run natural language query

Example:

show all user names

## ⚠️ Limitations

- SQL parsing is rule-based, not a full SQL parser
- Complex queries (nested queries, aggregations) may not be fully validated
- LLM output may still be imperfect
- Duplicate data may occur if CSV is loaded multiple times

## 🧠 Design Decisions

- Strict separation of concerns
- Validator protects database from LLM errors
- LLM is treated as a helper, not the system core
- Modular design enables independent testing

## 🤖 Use of AI

AI was used for:

- Code review and refinement
- Debugging assistance
- Improving test coverage

AI was NOT used to:

- Generate full modules without understanding
- Replace core system design decisions

All code was reviewed and understood before submission.

## 📌 Conclusion

This project demonstrates how LLMs can be integrated into data systems safely by combining:

- Strong validation
- Modular architecture
- Controlled execution

The system remains correct even when the LLM is wrong.
