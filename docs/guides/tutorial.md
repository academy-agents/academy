# Academy Tutorial
---
This tutorial provides a step by step introduction of Academy, from building a simple "Hello World" agent to coordinating a distributed, multi-agent game. The tutorial is structured as 5 modules that are cumulative --- the solution to the previous module provides the necessary information to understand the next module.

*Estimated Time: 40 minutes*

---
## Setup

#### Install Tutorial Package

Clone the repository from github and install the tutorial package. The package will automatically install `academy` as well.  The tutorial requires `python>=3.12`. We recommend completing the tutorial inside a virtual environment.
```
git clone git@github.com:academy-agents/academy-tutorial.git
cd academy-tutorial
python -m venv venv
. ./venv/bin/activate
pip install .
```
This will set up a local environment to complete the tutorial.

#### (Optional) Set-up Globus Compute Endpoint
The tutorial also contains a walk through of writing distributed applications with `academy` and `globus compute`. You will need to follow the instructions to setup a [globus compute endpoint](https://globus-compute.readthedocs.io/en/latest/quickstart.html#deploying-an-endpoint) or provide the `endpoint_id` of an endpoint you have access to. In the endpoint environment, you will also need to install the `academy-tutorial` package.
To use the endpoint with the tutorial:
```
export ACADEMY_TUTORIAL_ENDPOINT=<endpoint_id>
```

(If you are completing this tutorial with the Academy team, we will provide access to a Globus Compute Endpoint that you can use.)

---
## Helpful links

* [Example](https://docs.academy-agents.org/stable/#example): shows basic usage of most classes and methods needed in this tutorial
* [Getting started guide](https://docs.academy-agents.org/main/get-started/): an annotated example demonstrating basic features of Academy
* [API reference](https://docs.academy-agents.org/main/api/): detailed reference for Academy

---
## Module 01: Simple Actor-Client Application:
In the first module, we will write a simple example of launching a stateful actor and invoking actions from a user program.

The starter code contains an outline of an `Agent` class and a `Manager` class. In the `Counter` agent class add two new functions `increment` and `get_count` annotated with `@action` decorator.
In the `manager` context, first fill in the `factory` and the `executors` arguments. Then use `manager.launch` to launch the counter agent, and create a handle to the newly launched agent. To verify that the agent launched and is running call the `increment` and `get_count` methods of the agent using the handle.

To run the script:
```
python run-01.py
```

---
## Module 02: Adding "Autonomy"
Academy allows `agents` to express autonomous action using the `@loop` decorator. This can be used to monitor and respond to changing state or environment.

Using the completed code from Module 01, change the increment method to a loop that increments the value of count every one second.
To observe that it is working, change the `increment` call within the manager context to a `asyncio.sleep`. The `agent.count` should increment for how ever long the sleep is called.

To run the script:
```
python run-02.py
```

---
## Module 03: Agent-Agent Communication
Academy allows you to build multi-agent systems. Agent `Handle`s can be passed to other agents (or created by other agents) to allow one agent to invoke another.

The starter code for Module 03 contains a coordinator class that should lower and reverse a string using two other agents. Fill in the constructor to accept `Handle`s to the `Lowerer` and `Reverser` agents and store them as part of the `Coordinator` state. Then fill in the `process` action to invoke each handle.
In the `manager` context, launch the `Coordinator` agent. You can pass arguments to the `Coordinator`constructor using the `args=` argument of `manager.launch`

To run the script:
```
python run-03.py
```

---
## Module 04: Distributing Computation
Academy integrates with a cloud hosted exchange and `globus compute` to build distributed agentic systems.
In the starter code for Module 04, fill in the constructor of an executor used to launch agents with the `GCExecutor` (Globus Compute Executor) or a `ProcessPoolExecutor` if you do not have a Globus Compute Endpoint available. Then complete the Manager context with the appropriate fields. For the `factory` use the `HttpExchangeFactory` pointed at our hosted exchange `https:exchange.academy-agents.org` with `auth_method='globus'`. For the `executors` field use the `GCExecutor` or `ProcessPoolExecutor` you created above.
This will enable you to launch the agents across distributed resources.

Globus Compute is a FaaS platform that allows you to bring your own compute resource. In `run-compute-function.py` we show an example of running a remote function.

The script also uses a cloud hosted exchange. The exchange is authenticated with [globus auth](https://www.globus.org/platform/services/auth), and federated identification provider. When you run the script you will be asked to give permissions for the `academy` client to use the exchange, and authenticate your token. After you provide the permissions, a token will be created that the `academy` client uses to communicate with the exchange.

To run the script:
```
python run-04.py
```

---
## Module 05: Battleship
Finally, we use `academy` to build a battleship game. Battleship consists of two agents --- a `coordinator` the manages the game state, and multiple instance of the `player` agent which implements a strategy.

Implement the player strategy for guessing where opponent ships are and for placing ships on your board.

To run the script:
```
python run-05.py
```
