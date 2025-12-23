## Molecular Design Using Parallel Reasoning

This example illustrates combing multiple LLM-powered reasoners with simulations and parallel execution. Here, a team of agents is deployed to explore different elements of the chemical space, searching for molecules with high ionization energy.
[**LangGraph**](https://docs.langchain.com/oss/python/langgraph/overview)  is used within a single reasoner to coordinate multi-step prompting and tool use, while **Academy** is used to launch and coordinate agents with autonomous behavior in parallel.

## Setup

This example requires chemistry packages that must either be built from source or installed with `conda/mamba`. We recommend creating a separate environment to install the packages for this library.

```bash
$ mamba create -f environment.yaml
```

This will also install the `mol_design_agents.py` file from the package.  This is necessary in order to serialize agents for distribution with Parsl.

## High-Level Overview

We start in `run-07.py`. The program launches parallel agents using Parsl. Specifically, the `parsl.concurrent.ParslPoolExecutor` turns any Parsl configuration into a `concurrent.futures.Executor` compatible interface. To create a `ParslPoolExecutor`, we first define a Parsl configuration, then pass that configuration to the executor.
```python
config = Config(
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(
                worker_init=(
                    f'cd {os.getcwd()}conda activate ./mol-design;'
                ),
            ),
            max_workers_per_node=2,
        ),
    ],
)
executor = ParslPoolExecutor(config)
```

A configuration defines the mechanism used to launch parallel tasks. This configuration uses the `HighThroughputExecutor` and the `LocalProvider` to launch tasks as processes on the local node. For more information about configuring Parsl, please look at the [documentation](https://parsl.readthedocs.io/en/latest/userguide/configuration/index.html).

We use the executor the we created above to launch agents using the `Manager` interface.
```python
async with await Manager.from_exchange_factory(
    factory=RedisExchangeFactory('localhost', 6379),
    executors=executor,
) as manager:
```
Note that we also must use a exchange `factory` that is compatible with the `executors` that we specify. Here we use the `RedisExchangeFactory`. To launch this on an HPC site, you would need to change the address of the `Redis` instance to something accessible from all of the nodes in the job, likely the ip-address of the login node or the head node of your job (where you started `Redis`).

## Agent Overview

We use each Agent to perform parallel reasoning and exploration. We start by initializing different LLMs for the agent to use.
```
   async def agent_on_startup(self) -> None:
        n_workers: int
        if sys.platform == 'darwin':  # pragma: no cover
            n_workers = 2
        else:
            n_workers = max(
                len(os.sched_getaffinity(0)) - 1,
                1,
            )  # Get cores we are assigned to
        self.pool = ProcessPoolExecutor(max_workers=n_workers)

        tools = [tool(self.compute_ionization_energy)]
        self.reasoning_llm = ChatOpenAI(model=self.reasoning_model)
        self.generation_llm = ChatOpenAI(model=self.generation_model)
        self.tools_by_name = {tool.name: tool for tool in tools}
        self.llm_with_tools = self.generation_llm.bind_tools(tools)
```

We then use a `@loop` to continuously search the chemical space while the agent is running. To orchstrate multi-turn interactions with the LLM, we use `LangGraph`.
```
@loop
async def conduct_simulation_campaign(...):
    ...
    agent_builder = StateGraph(SearchState)

    # Add nodes
    agent_builder.add_node('plan', plan)
    agent_builder.add_node('tool_calling', tool_calling)
    agent_builder.add_node('simulate', simulate)
    agent_builder.add_node('conclude', conclude)
    agent_builder.add_node('critique', critique)
    agent_builder.add_node('update', update)

    # Add edges to connect nodes
    agent_builder.add_edge(START, 'plan')
    agent_builder.add_edge('plan', 'tool_calling')
    agent_builder.add_edge('tool_calling', 'simulate')
    agent_builder.add_edge('simulate', 'conclude')
    agent_builder.add_conditional_edges(
        'conclude',
        should_continue,
        ['critique', END],
    )
    agent_builder.add_edge('critique', 'update')
    agent_builder.add_edge('update', 'tool_calling')

    # Compile the agent
    agent = agent_builder.compile()

    # Run until the agent is terminated
    await agent.ainvoke(self.search_state, {'recursion_limit': 10000})
```

## What this example shows

- Agents as the axis of scaling in an HPC context for parallel, autonomous behavior
- Using the `ParslPoolExecutor` to launch agents

## Notes and Extensions

- Agents can receive insights and information learned by other agents that may be applicable.
- The tool calling can be finicky depending on the LLM used --- incorrect formatting of tool calls led to poor performance during testing.
- The `HybridExchangeClient` uses HPC interconnects to communicate between agents, and can accelerate agents when lots of data is communicated, or communication happens at a high frequency
