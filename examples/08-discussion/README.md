# Multi-Agent Group-Chat

This example illustrates how LLM powered agents can be combined to form a group chat. Here **Academy** supplies the communication and invocation mechanisms . The agents can be deployed across processes, nodes, or heterogeneous sites [Example 04](https://github.com/academy-agents/academy/tree/main/examples/04-execution) and equipped with different tools [Example 06](https://github.com/academy-agents/academy/tree/main/examples/06-llm)

> Warning: This example requires access to an OpenAI compatible language model API

This module contains additional dependencies to langchain and langchain-openai.

```
pip install langchain>=1.0
pip install langchain-openai
```

## High-Level Overview

THe program creates two types of agents:
- **`GroupChatAgent`**, which plays the role of a specific participant in a multi-agent conversation.
- **`RoundRobinGroupChatManager`** which invokese each agent in a predined order. In the manager, we also use Academy's ability to create autonomous behavior to asynchronously supervize the group chat. Periodically, the Manager reviews the conversation to ensure agents are making progress. This happens without interrupting the agents or input from the user.

## What This Example Demonstrates

- How to combine primitives for action invocation, control loops, and agent state to create llm-based multi-agent systems.
- Autonomous behavior and parallelism allowing more powerful Agent management.
