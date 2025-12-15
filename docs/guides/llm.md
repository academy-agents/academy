# LLM Agents
There are a variety of ways to create LLM agents within Academy.

> This page is still under construction, more details and examples to come!

## LLM as an Orchestrator (Agents as Tools)

Language models can be used as the central coordinator in the workflow, where they are used to pick which agents to invoke. In Academy, this means that the language model needs to (1) be wrapped in an agent, and (2) needs to be able to invoke actions on other Agents.


```
from academy.handle import Handle
from langchain.tools import tool, Tool

def make_sim_tool(handle: Handle[MySimAgent]) -> Tool:
    @tool
    async def compute_property(smiles: str) -> float:
        """Compute molecule property."""
        return await handle.compute_property(smiles)
    return compute_property

tool = make_sim_tool(agent_handle)
print(tool.args_schema.model_json_schema())
```

The LLM needs to be explicitly passed a tool because internally langchain uses the documentation and the signature, which are not available on the handle. This also meanse that tools must be defined dynamically or a specific wrapper is needed for each tool to specify the documentation.

For the complete code of Langchain interacting with Academy agents, please look at the [example](https://github.com/academy-agents/academy/tree/main/examples/06-llm) included in the repo.

## Multi-agent Discussion with LLMs

### Using specialized LLMsf

One advantage of distributing agents as


## Connecting to Academy via MCP
MCP is a protocol for connecting models to tools. If your language model LLM knows how to call tools via an MCP server, this can be used as an entry point into the Academy ecosystem.
