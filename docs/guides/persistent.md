# Building Persistent Agents

Academy allows you to build agents as "micro-services" for scientific workflows --- the same agent can outlive a single application and can be reused in different contexts. in this guide, we walk through common techniques that you can use to deploy persistent agents and problems you may encounter.

!!! warning

    This page is still under construction, more details and examples to come!

## Running an Agent Without a Manager
The most common (and recommended) way to run agentic applications with Academy is to use the [`Manager`][academy.manager.Manager] class. However, the manager class explicitly ties the lifetime of your agents to the lifetime of your manager.

To create an agent without using a `manager`, we first need to create a persistent mailbox for the agent. We can use the `exchange_client.register_agent` functionality. Note, for persistent agents you must use a `ExchangeFactory` that lives outside of process memory, so the `RedisExchangeFactory`, `HybridExchangeFactory` or `HttpExchangeFactory`. We then save the `registration` with the arguments we are going to need to start the agent to a file using `pickle`.

```
from academy.exchange import HttpExchangeFactory
from academy.manager import _RunSpec
from academy.runtime import RuntimeConfig

async def main():
    factory = HttpExchangeFactory()
    async with await factory.create_user_client() as client:
        registration = client.register_agent(MyAgentClass)
        spec = _RunSpec(
            agent=MyAgentClass,
            agent_args=<AgentArgs>,
            agent_kwargs=<LaunchKwargs>,
            exchange_factory=exchange_factory,
            registration=registration,
            config=RuntimeConfig(terminate_on_error=False),
        )

        with open('~/local/share/agents/MyAgentSpec.pkl', 'wb') as fp:
            pickle.dump(spec, fp)
```

Once we have created the registration and the spec, we can launch the agent as a separate [script][academy.run].
```
python -m academy.run --spec <path to spec file>
```

## Running an Agent with SystemD

By saving the `_RunSpec` as a file, we can set up a systemd service to keep the `Agent` alive. Note, these are general instructions for setting up an agent as a service, you may need to speak to your site administrator about system specific policies.

First we define a unit file and save it at `~/.config/systemd/user/`

For instance:
```
[Unit]
Description=MyAcademyAgent
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=centos
ExecStart=~/.venv/bin/python -m academy.run --spec <path to spec>
```

Once you have created you unit file, you can start the service:
```
systemctl --user start <service_name>
```
