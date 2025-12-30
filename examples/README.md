# Examples

Examples of patterns and techniques for building agents with Academy.

**01: Actor Client**. Simple example of launching a stateful actor and invoking actions on that actor from a user program.

**O2: Control Loop**. Example of an agent with a control loop for autonomous behavior.

**03: Agent to Agent Interaction**. Trivial text processing pipeline where one Coordinator agent invokes actions on two other agents.

**04: Agent Execution Methods**. This example extends Example 03 to demonstrate executing agents in different processes using a concurrent.futures.Executor and an external message exchange.

**05: Parsl Agent**. An agent that delegates actions to a [Parsl]('https://parsl-project.org) program.

**06: LLM-calling Agent**. An agent-of-agents example in which an LLM-powered orchestrator agent delegates scientific work to a simulation agent via well-defined actions, with [LangChain](https://docs.langchain.com/oss/python/langchain/overview) used to handle reasoning and tool selection.
