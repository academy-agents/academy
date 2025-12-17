# LLM Agents
There are a variety of ways to create LLM agents within Academy.

!!! warning

    This page is still under construction, more details and examples to come!

## LLM as an Orchestrator (Agents as Tools)

A language model can be used as the central coordinator in a workflow, where it is used to pick which agent(s) to invoke.
In Academy, this means that the language model needs to be (1) wrapped in an agent, and (2) able to invoke actions on other Agents.

You can wrap an action invocation as a Tool call from a multi-LLM orchestration framework (e.g., LangChain or pydanticAI).
For example, see the following code from [the LLM example](https://github.com/academy-agents/academy/tree/main/examples/06-llm):

```
from academy.handle import Handle
from langchain.tools import tool, Tool

def make_sim_tool(handle: Handle[MySimAgent]) -> Tool:
    ...

    @tool
    async def compute_ionization_energy(smiles: str) -> float:
        """Compute the ionization energy of a molecule."""
        return await handle.compute_ionization_energy(smiles)

    return compute_ionization_energy

```

The LLM needs to be explicitly passed a tool because internally langchain uses the doc-string and the signature, which are not available on the handle. This also means that tools must be defined dynamically or a specific wrapper is needed for each tool to specify the documentation.

Once the action is wrapped in a tool call, Langchain can call actions on (potentially remote) agents, allowing your language model access to research infrastrucutre.

```
async def main() -> int:
    ...
    async with await Manager.from_exchange_factory(
        executors=<Insert your executor>
        factory=<Insert your factory>
    ) as manager:
        simulator = await manager.launch(MySimAgent)
        llm = ChatOpenAI(
            model=self.model,
            api_key=self.access_token,
            base_url=self.base_url,
        )

        tools = [make_sim_tool(agent) for agent in self.simulators]
        langchain_agent = create_agent(llm, tools=tools)
        result = await langchain_agent.ainvoke(
            {
                'messages': [{
                    'role': 'user',
                    'content': 'What is the simulated ionoization energy of benzene?'
                }]
            },
        )
        print(result)

    return 0
```

This example still uses the client script to interact with the simulation agent through langchain. It is also possible to make the orchestration agentic and distributed (i.e. an Academy agent). For example, if the orchestrator relies on a self-hosted language model available only within a certain resource, or if the agent is managing resources (such as the orchestrator running on a login node and starting up agents on compute nodes.) To do this, we can wrap the Langchain code inside an agent:

```
class Orchestrator(Agent):
    """Orchestrate a scientific workflow."""

    def __init__(
        self,
        model: str,
        access_token: str,
        simulators: list[Handle[MySimAgent]],
        base_url: str | None = None,
    ):
        self.model = model
        self.access_token = access_token
        self.base_url = base_url
        self.simulators = simulators

    async def agent_on_startup(self) -> None:
        llm = ChatOpenAI(
            model=self.model,
            api_key=self.access_token,
            base_url=self.base_url,
        )

        tools = [make_sim_tool(agent) for agent in self.simulators]
        self.react_loop = create_agent(llm, tools=tools)

    @action
    async def answer(self, goal: str) -> str:
        """Use other agents to answer questions about molecules."""

        # This call runs the ReACT loop, in which:
        #   1) the LLM is used to determine which tool to call,
        #   2) the tool is called (by messaging the Academy agent)
        return await self.react_loop.ainvoke(
            {'messages': [{'role': 'user', 'content': goal}]},
        )
```

For the complete code of LangChain interacting with Academy agents, please look at the [example](https://github.com/academy-agents/academy/tree/main/examples/06-llm) included in the repo.

## Multi-agent Discussion with LLMs

Coming soon!

### Using Specialized LLMs

Coming soon!

## Connecting to Academy via MCP
MCP is a protocol for connecting models to tools. If your language model LLM knows how to call tools via an MCP server, this can be used as an entry point into the Academy ecosystem. For details on how to use Academy with an MCP server, please see the [academy-extensions documentation](https://academy-agents.org/academy-extensions/latest/guides/mcp/).
