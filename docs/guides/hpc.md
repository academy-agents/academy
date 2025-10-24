# Building HPC Agents
Academy supports deploying agents and running tasks on HPC resource. This guide walks through different patterns of using HPC resources with Academy.

## Launching Agents with Globus Compute

Academy can be combined with Globus Compute to deploy agents onto remote resources.
Globus Compute is a function-as-a-service provider with a bring-your-own compute model.

A Globus Compute endpoint can be deployed on an HPC resource using the `globus_compute_endpoint` package:
```bash
pip install globus-compute-endpoint
globus-compute-endpoint configure
globus-compute-endpoint start <ENDPOINT_NAME>
```
When you start the Globus Compute endpoint, the script will output an `endpoint_id`. For more information on deployoing and configuring a Globus Compute endpoint please refer to the [Globus Compute documentation](https://globus-compute.readthedocs.io/en/latest/quickstart.html#deploying-an-endpoint).

To launch agents on the Globus Compute endpoint, we can use the [Globus Compute `executor`](https://globus-compute.readthedocs.io/en/latest/sdk/executor_user_guide.html) with the [`Manager`][academy.manager.Manager] class. The following script initializes a `executor` pointing to `<endpoint_id>` and passes the executor to a `Manager` to launch agents. It also connects the Manager and agent to the cloud hosted exchange at https://exchange.academy-agents.org using the [`HttpExchangeFactory`][academy.exchange.HttpExchangeFactory] for authenticated inter-site communication.

```python title="globus_compute_example.py" linenums="1"
## Deploying Agents with Globus Compute
...

from academy.exchange.cloud.client import HttpExchangeFactory
from academy.manager import Manager
from globus_compute_sdk import Executor as GCExecutor

EXCHANGE_ADDRESS = 'https://exchange.academy-agents.org'

async def main() -> int:
    executor = GCExecutor(<endpoint_id>)
    async with await Manager.from_exchange_factory(
        factory=HttpExchangeFactory(
            EXCHANGE_ADDRESS,
            auth_method='globus',
        ),
        executors=executor,
    ) as manager:
        hdl = await manager.launch(<Agent>)
        ...
```
For the full code for this example, refer to the [tutorial](https://github.com/academy-agents/academy-tutorial/blob/main/solutions/04-execution/run-04.py)

## Running Actions With Parsl

Academy Agents can also manage a pool of resources to run actions on HPC facilities. With this pattern an Agent is deployed on the login-node of a cluster (either using Globus Compute as above, or co-located with the user-script that is located in the same place). An Agent can then use technique it chooses to allocate resources and run tasks on the cluster.

The following script shows a simple example of invoking a [Parsl](https://parsl-project.org/) task within an Academy action.
```python title="parsl_example.py" linenums="1"
## Running actions with parsl
...
import asyncio
import parsl
from parsl import Config
from academy.agent import Agent, action


@parsl.python_app
def expensive_task():
    # Do expensive task
    return 42

class SimulationAgent(Agent):
    def __init__(self):
        self.config = Config(
            executors=[
                HighThroughputExecutor()
            ],
        )

    async def agent_on_startup(self) -> None:
        self.dfk = parsl.load(self.config)

    async def agent_on_shutdown(self) -> None:
        self.dfk.cleanup()
        self.dfk = None
        parsl.clear()

    @action
    async def run_expensive_task(self) -> None:
        return await asyncio.wrap_future(expensive_task())

```
The configuration will submit jobs to the cluster using Slurm, and run tasks using the HighThroughputExecutor. For information on configuring Parsl, please see the Parsl [docs](https://parsl.readthedocs.io/en/stable/userguide/configuration/index.html).
