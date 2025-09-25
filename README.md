# Academy: Federated Actors and Agents

![PyPI - Version](https://img.shields.io/pypi/v/academy-py)
[![tests](https://github.com/academy-agents/academy/actions/workflows/tests.yml/badge.svg)](https://github.com/academy-agents/academy/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/academy-agents/academy/main.svg)](https://results.pre-commit.ci/latest/github/academy-agents/academy/main)

Academy is a modular and extensible middleware for building and deploying stateful actors and autonomous agents across distributed systems and federated research infrastructure.
In Academy, you can:

* ⚙️  Express agent behavior and state in code
* 📫 Manage inter-agent coordination and asynchronous communication
* 🌐 Deploy agents across distributed, federated, and heterogeneous resources

## Installation

Academy is available on [PyPI](https://pypi.org/project/academy-py/).

```bash
pip install academy-py
```

## Example

Agents in Academy are defined by an `Agent` class containing `@action`-decorated methods that can be invoked by users or peer agents and `@loop`-decorated methods that execute the autonomous control loops of the agent.

The below sensor monitoring agent periodically reads a sensor in the `monitor()` loop and processes the reading if a threshold is met.
Users or agents can invoke the `get_last_reading()` and `set_process_threshold()` actions remotely to interact with the monitor agent.

```python
import asyncio
from academy.agent import Agent, action, loop

class SensorMonitorAgent(Agent):
    def __init__(self) -> None:
        super().__init__()
        self.last_reading: float | None = None
        self.process_threshold: float = 1.0

    @action
    async def get_last_reading(self) -> float | None:
        return self.last_reading

    @action
    async def set_process_threshold(self, value: float) -> None:
        self.process_threshold = value

    @loop
    async def monitor(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            value = await read_sensor_data()
            self.last_reading = value:
            if value >= self.process_threshold:
                await process_reading(value)
            await asyncio.sleep(1)
```

Users and agents communicate asynchronously through *handles*, sending messages to and receiving messages from a mailbox managed by an *exchange*.
The *manager* abstracts the remote execution and management of agents using [executors](https://docs.python.org/3/library/concurrent.futures.html).

```python
from academy.exchange import LocalExchangeFactory
from academy.manager import Manager
from concurrent.futures import ThreadPoolExecutor

async with await Manager.from_exchange_factory(
    factory=LocalExchangeFactory(),  # Replace with other implementations
    executors=ThreadPoolExecutor(),  # for distributed deployments
) as manager:
    agent_handle = await manager.launch(SensorMonitorAgent)

    await agent_handle.set_process_threshold(2.0)
    await asyncio.sleep(5)
    value = await agent_handle.get_last_reading()
    print(value)

    await manager.shutdown(agent_handle, blocking=True)
```

Learn more about Academy in [Getting Started](https://docs.academy-agents.org/latest/get-started).

## What can be an agent?

In Academy, an agent is a primitive entity that (1) has internal state, (2) performs actions, and (3) communicates with other agents.

This allows for range of agent implementations—Academy agents are building blocks for constructing more complex agent-based systems.

For example, Academy can be use to create the following:

* **Stateful Actors:** Actors manage their own data and respond to requests in a distributed system.
* **LLM Agents:** Integrate LLM-based reasoning and tool calling.
* **Embodied Agents:** The "brain" controlling a robot or simulated entity where action are translated into motor commands or environment manipulations.
* **Computational Units:** Encapsulate a specific computational task, like running a simulation, processing data, or training a machine learning model.
* **Orchestrators:** Manage or coordinate the activities of other agents, distributing tasks and monitoring progress.
* **Data Interfaces:** Interact with external data sources, such as databases, file systems, or sensors, providing a consistent interface for data access and manipulation.

## Why Academy?

Academy offers a powerful and flexible framework for building sophisticated, distributed agent-based systems, particularly well-suited for the complexities of scientific applications.
Here's what makes Academy valuable:

* **Stateful Agents:** Academy enables agents to maintain state, which is crucial for managing long-running processes, tracking context across steps, and implementing agents that need to "remember" information.
* **Agent Autonomy:** Academy allows agents to have autonomous control loops, empowering them to make decisions, react to events, and execute tasks independently.
* **Flexible Deployment:** Academy provides tools for managing agent deployment, communication, and coordination in complex environments such that applications can leverage heterogeneous, distributed, and federated resources.
* **Foundation for Sophisticated Applications:** Academy primitives offer a strong foundation for building highly specialized and sophisticated agent-based systems that go beyond standard LLM use cases, allowing for fine-grained control and optimization tailored to specific scientific applications.

## Citation

The Academy preprint is available on [arXiv](https://arxiv.org/abs/2505.05428).

```bibtex
@misc{pauloski2025academy,
    title = {{E}mpowering {S}cientific {W}orkflows with {F}ederated {A}gents},
    author = {J. Gregory Pauloski and Yadu Babuji and Ryan Chard and Mansi Sakarvadia and Kyle Chard and Ian Foster},
    archivePrefix = {arXiv},
    eprint = {2505.05428},
    primaryClass = {cs.MA},
    url = {https://arxiv.org/abs/2505.05428},
    year = {2025},
}
```
