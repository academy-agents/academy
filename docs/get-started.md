# Getting Started

## Installation

You can install Academy with `pip` or from source.
We suggest installing within a virtual environment (e.g., `venv` or Conda).
```bash
python -m venv venv
. venv/bin/activate
```

*Option 1: Install from PyPI:*
```bash
pip install academy-py
```

*Option 2: Install from source:*
```bash
git clone git@github.com:academy-agents/academy
cd academy
pip install -e .  # -e for editable mode
```

## A Basic Example

The following script defines, initializes, and launches a simple agent that performs a single action.
Click on the plus (`+`) signs to learn more.

```python title="example.py" linenums="1"
import asyncio
from concurrent.futures import ThreadPoolExecutor
from academy.agent import Agent, action
from academy.exchange import LocalExchangeFactory
from academy.logging import init_logging
from academy.manager import Manager

class ExampleAgent(Agent):  # (1)!
    @action  # (2)!
    async def square(self, value: float) -> float:
        return value * value

async def main() -> None:
    init_logging('INFO')

    async with await Manager.from_exchange_factory(  # (3)!
        factory=LocalExchangeFactory(),  # (4)!
        executors=ThreadPoolExecutor(),  # (5)!
    ) as manager:
        agent_handle = await manager.launch(ExampleAgent())  # (6)!

        result = await agent_handle.square(2)  # (7)!
        assert result == 4

        await agent_handle.shutdown()  # (8)!

if __name__ == '__main__':
    asyncio.run(main())
```

1. Agents are with derived classes of [`Agent`][academy.agent.Agent].
2. Async agent methods decorated with [`@action`][academy.agent.action] can be invoked remotely by user programs and other agents. An agent can call action methods on itself as normal methods.
3. The [`Manager`][academy.manager.Manager] is a high-level interface that reduces boilerplate code when launching and managing agents. It will also manage clean up of resources and shutting down agents when the context manager exits.
4. The [local exchange][academy.exchange.LocalExchangeFactory] manages message passing between users and agents running in a single process. Factories are used to create clients to the exchange.
5. The manager uses an [`Executor`][concurrent.futures.Executor] to run agents concurrently across parallel/distributed resources. Here, a [`ThreadPoolExecutor`][concurrent.futures.Executor] runs agents in different threads of the main process.
6. An instantiated agent (here, `ExampleAgent`) can be launched with [`Manager.launch()`][academy.manager.Manager.launch], returning a handle to the remote agent.
7. Interact with running agents via a [`Handle`][academy.handle.Handle]. Invoking an action returns the result.
8. Agents can be shutdown via a handle or the manager.

Running this script with logging enabled produces the following output:
```
$ python example.py
INFO (root) Configured logger (stdout-level=INFO, logfile=None, logfile-level=None)
INFO (academy.manager) Initialized manager (UserId<6e890226>; ThreadExchange<4401447664>)
INFO (academy.manager) Launched agent (AgentID<ad6faf7e>; Agent<ExampleAgent>)
INFO (academy.runtime) Running agent (AgentID<ad6faf7e>; Agent<ExampleAgent>)
INFO (academy.runtime) Shutdown agent (AgentID<ad6faf7e>; Agent<ExampleAgent>)
INFO (academy.manager) Closed manager (UserId<6e890226>)
```

## Control Loops

Control loops define the autonomous behavior of a running agent and are created by decorating a method with [`@loop`][academy.agent.loop].

```python
from academy.agent import loop

class ExampleAgent(Agent):
    @loop
    async def counter(self, shutdown: asyncio.Event) -> None:
        count = 0
        while not shutdown.is_set():
            print(f'Count: {count}')
            count += 1
            await asyncio.sleep(1)
```

All control loops are started in separate tasks in the event loop when an agent is executed, and run until the control loop exits or the agent is shut down, as indicated by the `shutdown` event.
If an agent shuts down before the control loops exit, the corresponding task will be cancelled.

## Agent to Agent Interaction

Agent handles can be passed to other agents to facilitate agent-to-agent interaction.
Here, a `Coordinator` is initialized with handles to two other agents implementing the `Lowerer` and `Reverser` agents, respectively.

```python
from academy.agent import action
from academy.agent import Agent
from academy.handle import Handle

class Coordinator(Agent):
    def __init__(
        self,
        lowerer: Handle[Lowerer],
        reverser: Handle[Reverser],
    ) -> None:
        super().__init__()
        self.lowerer = lowerer
        self.reverser = reverser

    @action
    async def process(self, text: str) -> str:
        text = await self.lowerer.lower(text)
        text = await self.reverser.reverse(text)
        return text


class Lowerer(Agent):
    @action
    async def lower(self, text: str) -> str:
        return text.lower()


class Reverser(Agent):
    @action
    async def reverse(self, text: str) -> str:
        return text[::-1]
```

After launching the `Lowerer` and `Reverser`, the respective handles can be used to initialize the `Coordinator` before launching it.

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from academy.agent import Agent, action
from academy.exchange import LocalExchangeFactory
from academy.logging import init_logging
from academy.manager import Manager

async def main() -> None:
    init_logging(logging.INFO)

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
        executors=ThreadPoolExecutor(),
    ) as manager:
        lowerer = await manager.launch(Lowerer)
        reverser = await manager.launch(Reverser)
        coordinator = await manager.launch(
            Coordinator,
            args=(lowerer, reverser),
        )

        text = 'DEADBEEF'
        expected = 'feebdaed'

        logger.info('Invoking process("%s") on %s', text, coordinator.agent_id)
        result = await coordinator.process(text)
        assert result == expected
        logger.info('Received result: "%s"', result)

if __name__ == '__main__':
    asyncio.run(main())
```

## Distributed Execution

The prior examples have launched agent in threads of the main process, but in practice agents are launched in different processes, possibly on the same node or remote nodes.
The prior example can be executed in a distributed fashion by changing the executor and exchange to implementations which support distributed execution.
Below, a [Redis server](https://redis.io/){target=_blank} server (via the [redis exchange][academy.exchange.RedisExchangeFactory]) is used to support messaging between distributed agents executed with a [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor].

```python
from concurrent.futures import ProcessPoolExecutor
from academy.exchange import RedisExchangeFactory

async def main() -> None:
    async with Manager.from_exchange_factory(
        exchange=RedisExchangeFactory('<REDIS HOST>', port=6379),
        executors=ProcessPoolExecutor(max_processes=4),
    ) as manager:
        ...
```
