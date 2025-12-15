from __future__ import annotations

import asyncio
import logging

from langchain.agents import create_agent
from langchain.tools import Tool
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from academy.agent import action
from academy.agent import Agent
from academy.exchange import LocalExchangeFactory
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


class MySimAgent(Agent):
    """Agent for conducting simulations."""

    @action
    async def compute_property(self, smiles: str) -> float:
        """Compute the ionization energy for the given molecule."""
        return 0.5


def make_sim_tool(handle: Handle[MySimAgent]) -> Tool:
    """Wraps an academy handle in a langchain tool.

    Note: Since the documentation of the tool is used by the language
    model, a specific wrapper method may need to be written per agent.
    """

    @tool
    async def compute_property(smiles: str) -> float:
        """Compute molecule property."""
        return await handle.compute_property(smiles)

    return compute_property


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
    async def hypothesize(self, goal: str) -> str:
        """Use other agents to hypothesize molecules."""

        return await self.react_loop.ainvoke(
            {'messages': [{'role': 'user', 'content': goal}]},
        )


async def main() -> int:
    init_logging(logging.INFO)

    model = await asyncio.to_thread(input, 'Please input a model name: ')
    token = await asyncio.to_thread(input, 'Please input an access token: ')
    url_input = await asyncio.to_thread(
        input,
        '(Optionally) Input a model api url: ',
    )
    url = url_input if len(url_input) > 0 else None

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
    ) as manager:
        simulator = await manager.launch(MySimAgent)
        orchestrator = await manager.launch(
            Orchestrator,
            kwargs={
                'model': model,
                'access_token': token,
                'simulators': [simulator],
                'base_url': url,
            },
        )

        question = 'What is the simulated ionoization energy of benzene?'
        print(question)

        result = await orchestrator.hypothesize(question)

        print(result)

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
