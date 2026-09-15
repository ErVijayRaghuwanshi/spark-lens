If you're learning **FastMCP / MCP (Model Context Protocol)** for an AI Engineer interview, the easiest way to understand it is:

> **Tools = actions**
> **Resources = data/context**
> **Prompts = reusable instructions**

### 1. Tools — “Let the AI do something”

A **tool** is a function that the LLM can call to perform an action.

Examples:

* Query a database
* Call an API
* Search documents
* Create a ticket
* Calculate something

```python
from fastmcp import FastMCP

mcp = FastMCP("Demo Server")

@mcp.tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b
```

The model can decide:

```text
User: What is 10 + 20?

LLM → calls add(10, 20)
MCP Server → returns 30
LLM → "The answer is 30."
```

**Think:** `Tool = function/action`

---

### 2. Resources — “Give the AI some data”

A **resource** exposes data that an MCP client/LLM can read.

For example:

```python
@mcp.resource("config://app")
def get_config():
    return """
    Application: Payment Service
    Environment: Production
    Database: PostgreSQL
    """
```

A resource has a URI:

```text
config://app
```

The client can retrieve that resource and provide its contents as context to the model.

Other examples:

```text
file://documents/report.pdf
db://customers/123
config://application
```

**Think:** `Resource = data/context`

---

### 3. Prompts — “Give the AI a reusable instruction”

A **prompt** is a predefined prompt template exposed by the MCP server.

```python
@mcp.prompt
def code_review(code: str):
    return f"""
    Review the following Python code.

    Check for:
    1. Bugs
    2. Security issues
    3. Performance problems
    4. PEP8 violations

    Code:
    {code}
    """
```

Now the client can use the `code_review` prompt with different code.

**Think:** `Prompt = reusable instruction/template`

---

## Tools vs Resources vs Prompts

| MCP primitive | Purpose               | Example             |
| ------------- | --------------------- | ------------------- |
| **Tool**      | Perform an action     | `search_database()` |
| **Resource**  | Provide data/context  | `customer://123`    |
| **Prompt**    | Reusable instructions | `code_review(code)` |

A very easy mental model:

```text
             MCP Server
                 |
       ┌─────────┼─────────┐
       ↓         ↓         ↓
     TOOLS    RESOURCES  PROMPTS
       |         |         |
     DO        READ       GUIDE
   something   data       AI
```

### Interview answer

If an interviewer asks **“What are Tools, Resources and Prompts in MCP?”**, say:

> “MCP provides three main primitives. **Tools** are executable functions that allow an AI model to perform actions, such as querying a database or calling an API. **Resources** expose data or context through URIs, such as files, database records, or application configuration. **Prompts** are reusable prompt templates that provide predefined instructions for specific tasks. In short, tools allow the model to **act**, resources allow it to **access context**, and prompts help it **follow reusable instructions**.”

### FastMCP example

Putting all three together:

```python
from fastmcp import FastMCP

mcp = FastMCP("Customer Server")


# TOOL
@mcp.tool
def get_customer(customer_id: int):
    """Fetch customer information."""
    return {
        "id": customer_id,
        "name": "Vijay",
        "plan": "Premium"
    }


# RESOURCE
@mcp.resource("company://policy")
def company_policy():
    return """
    Premium customers receive 24/7 support.
    """


# PROMPT
@mcp.prompt
def customer_support(customer_id: int, issue: str):
    return f"""
    Help customer {customer_id} with this issue:

    {issue}

    Follow the company support policy.
    """


if __name__ == "__main__":
    mcp.run()
```

So in an **Agentic RAG** architecture, you might have:

```text
                LLM / Agent
                     |
                    MCP
                     |
        ┌────────────┼─────────────┐
        ↓            ↓             ↓
      Tools       Resources      Prompts
        ↓            ↓             ↓
   Search DB     Documents     RAG instructions
   Call APIs     Policies       Agent behavior
   Create ticket  Schemas       Support workflow
```

**One important interview point:** MCP itself is a **protocol**; **FastMCP is a Python framework/library that makes building MCP servers and clients easier.**
