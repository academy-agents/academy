from __future__ import annotations

import asyncio
import logging

from group_chat_agents import GroupChatAgent
from group_chat_agents import RoundRobinGroupChatManager
from langchain_openai import ChatOpenAI

from academy.exchange import LocalExchangeFactory
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


async def main() -> int:
    init_logging(logging.INFO)
    llm = ChatOpenAI(model='meta-llama/Llama-4-Scout-17B-16E-Instruct')
    agents = [
        GroupChatAgent(
            llm,
            role='Manager',
            prompt=(
                'Your are managing a team. Participate in the conversation '
                'with a user by coming up with subtasks for your team to '
                'solve. Your team consists of a junior assistant and a senior'
                ' engineer.'
            ),
        ),
        GroupChatAgent(
            llm,
            role='Assistant',
            prompt=(
                'You are an assistant. Participate in the conversation by '
                'taking care of small or trivial tasks needed to complete '
                'the users request.'
            ),
        ),
        GroupChatAgent(
            llm,
            role='Senior Engineer',
            prompt=(
                'You are a senior enginner. Participate in the conversation '
                'by assessing the Assistants results and checking them for '
                'accuracy.'
            ),
        ),
    ]

    async with await Manager.from_exchange_factory(
        factory=LocalExchangeFactory(),
    ) as manager:
        participants: list[Handle[GroupChatAgent]] = []
        for agent in agents:
            participants.append(await manager.launch(agent))

        supervisor = await manager.launch(
            RoundRobinGroupChatManager,
            kwargs={
                'participants': participants,
                'llm': llm,
            },
        )

        await supervisor.query(
            'What size parachute do I need to stop a 10kg plastic rocket from'
            " breaking when it's launched 300ft",
        )

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
