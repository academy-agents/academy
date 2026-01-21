# LLM Agents
There are a variety of ways to create LLM agents within Academy.

!!! warning

    This page is still under construction, more details and examples to come!

## LLM as an Orchestrator (Agents as Tools)

A language model can be used as the central coordinator in a workflow, where it is used to pick which agent(s) to invoke.
In Academy, this means that the language model needs to be (1) wrapped in an agent, and (2) able to invoke actions on other Agents.

You can wrap an action invocation as a Tool call from a multi-LLM orchestration framework (e.g., LangChain or pydanticAI).
For example, see the following code from [the LLM example](https://github.com/academy-agents/academy/tree/main/examples/06-llm):

```python
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

```python
async def main() -> int:
    ...
    async with await Manager.from_exchange_factory(
        executors=<Insert your executor>
        factory=<Insert your factory>
    ) as manager:
        simulator = await manager.launch(MySimAgent)
        llm = ChatOpenAI(
            model=model
        )

        tools = [make_sim_tool(simulator),]
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

```python
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

Multi-agent discussion are a popular technique to elicit higher level planning and reasoning from language models through collaboration. In the [parallel agents example](hpc.md), we use LangGraph to coordinate a multi-turn interaction with an LLM, however Academy can be used to coordinate this interaction. This allows (1) increased autonomy as agents can act independently and in parallel with other agents in the discussion, and (2) visibility through the exchange so a user or another agent can monitor the discussion.  In the following code (taken from this example in the repository [example](https://github.com/academy-agents/academy/tree/main/examples/08-discussion)), we demonstrate setting up and monitoring a multi-agent chat:

```python
class GroupChatAgent(Agent):
    ...

    def __init__(
        self,
        llm_or_agent: Any,
        role: str,
        prompt: str,
    ) -> None:
        self.role = role
        self.prompt = prompt
        self.llm = llm_or_agent
        self.peers: list[
            Handle[GroupChatAgent] | Handle[RoundRobinGroupChatManager]
        ] = []
        self.messages = [
            {'role': 'system', 'content': self.prompt},
        ]
```

The group chat agent defines a class for role-playing LLM agents. This class allows you to specify the LLM model, role and prompt for this participant in the discussion.

An group chat participant can be instantiated directly or through a subclass of this `GroupChatAgent`. Overriding the action `respond()` allows a specific participant to take other behavior on their turn in the conversation.

```python
class RoundRobinGroupChatManager(Agent):
    ...
    def __init__(  # noqa: PLR0913
        self,
        participants: list[Handle[GroupChatAgent]],
        llm: Any,
        ...
    ):
        ...
```
The other key agent in this example is the `RoundRobinGroupChatManager` which handles notifying all the participating agents of the chat, and calling the appropriate agent on it's turn in the conversation. The group chat manager user as LLM to decide weather to end the conversation based off the users initial inputs.

One key feature of this example is the supervizing capability of the manager.
```python
@loop
async def supervize(self, shutdown: asyncio.Event) -> None:
    """Background loop to make sure agents are making progress."""

    while not shutdown.is_set():
        await self.new_messages.wait()
        logger.info('Beginning supervior checks.')
        history = list(self.history)
        self.new_messages.clear()

        history_str = self._format_history(history)
        # Perform supervisory actions!
        supervize_response = await self.llm.ainvoke(
            [
                {
                    'role': 'system',
                    'content': self.supervisor_prompt.format(
                        conversation_history=history_str,
                    ),
                },
            ],
        )
        logger.info(f'Supervisor Response: {supervize_response.content}')
        if self._check_match(supervize_response.content, 'TERMINATE'):
            self.terminate.set()
            continue

        if self._check_match(supervize_response.content, 'PAUSE'):
            # Pause conversation
            self.running.clear()
            for participant in self.participants:
                await self.update_participant(history, participant)
            self.running.set()
```
Asynchronously to the rest of the conversation, the manager inspects the state of the discussion history to figure out if the discussion is in a loop. If it detects that the discussion is stuck, it can terminate the conversation, or it can update the prompt of any of the participants.

For the full code, and to run this conversation, please see the [example](https://github.com/academy-agents/academy/tree/main/examples/08-discussion) in the repository.

### Using Specialized LLMs

One reason to deploy agents on HPC systems is to potentially equip each agent with a specailized language model. Rather than setting up an inference service to handle each specialized model, models (i.e. the weight loaded into GPU memory) can be deployed as part of the agent itself.

## Connecting to Academy via MCP
MCP is a protocol for connecting models to tools. If your language model LLM knows how to call tools via an MCP server, this can be used as an entry point into the Academy ecosystem. For details on how to use Academy with an MCP server, please see the [academy-extensions documentation](https://academy-agents.org/academy-extensions/latest/guides/mcp/).
