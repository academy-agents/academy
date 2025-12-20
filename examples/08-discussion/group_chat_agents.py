from __future__ import annotations

import asyncio
import logging
import string
from typing import Any

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.handle import Handle

logger = logging.getLogger(__name__)


class GroupChatAgent(Agent):
    """A participant in a group chat.

    This class can be subclassed to design specific actions or
    instantiated with a role and a prompt

    Args:
        llm_or_agent: A Langchain llm instance, or agent.
        role: The role this participant has in the group chat.
        prompt: The system prompt to initialize the chat for the
            agent.
    """

    def __init__(
        self,
        llm_or_agent: Any,
        role: str,
        prompt: str,
    ) -> None:
        self.role = role
        self.prompt = prompt
        self.llm = llm_or_agent
        self.peers: list[
            Handle[GroupChatAgent] | Handle[RoundRobinGroupChatManager]
        ] = []
        self.messages = [
            {'role': 'system', 'content': self.prompt},
        ]

    @action
    async def add_peer(
        self,
        peer: Handle[GroupChatAgent] | Handle[RoundRobinGroupChatManager],
    ) -> None:
        """Add a peer to this agent.

        Peers receive any message sent by this agent.
        """
        self.peers.append(peer)

    @action
    async def get_role(self) -> str:
        """Get group chat role."""
        return self.role

    @action
    async def get_prompt(self) -> str:
        """Get group chat prompt."""
        return self.prompt

    @action
    async def update_prompt(self, new_prompt: str) -> None:
        """Update the prompt of this agent."""
        self.prompt = new_prompt
        self.messages[0] = {'role': 'system', 'content': self.prompt}

    @action
    async def receive(self, role: str, content: str) -> None:
        """Add a message to the conversation history.

        Args:
            role: Who in the group chat is sending the message.
            content: Contents of the message.
        """
        self.messages.append(
            {
                'role': 'user',
                'content': f'{role}: {content}',
            },
        )

    @action
    async def respond(self) -> str:
        """Invoke agent to respond to new messages."""
        response = await self.llm.ainvoke(self.messages)
        self.messages.append(
            {'role': 'assistant', 'content': response.content},
        )
        for peer in self.peers:
            await peer.receive(self.role, response.content)

        return response.content

    @action
    async def clear(self) -> None:
        """Clear the conversation history."""
        self.messages = self.messages[:1]


DEFAULT_STOPPING_PROMPT = (
    'You are supervising a conversation between role playing agents. '
    'Decide if the users request has been answered by the conversation.'
    "If it has return the single word 'STOP' otherwise return 'CONTINUE'"
    'Conversation:\n\n'
    '{conversation_history}'
)

DEFAULT_SUPERVISOR_PROMPT = (
    'You are supervising a conversation between role playing agents. '
    'Look at the conversation and decide if the agents are making progress.'
    'Specifically look for circles of repeated reasoning. If the conversation'
    "is not progressing, return the single word 'PAUSE' to edit the prompts"
    "of the agents or 'TERMINATE' to terminate the conversation. Otherwise "
    "return 'CONTINUE'."
    'Conversation:\n\n'
    '{conversation_history}'
)

DEFAULT_EDIT_PROMPT = (
    'You are supervising a conversation between role playing agents. '
    'The agents have stopped making progress. The current state of the'
    'conversation is:\n\n'
    '{conversation_history}\n'
    'The {role} agents instructions are: {prompt}. If this agent is impeding'
    ' the conversation or making a mistake, return a new instruction to the'
    ' agent that will be used as the system prompt. First identify how that'
    ' agent is failing and reason about why the new instruction would help '
    'correct the error that you observe. To return the new prompt, return '
    '### Prompt:` If the agent does not need modification return `CONTINUE`.'
)


class RoundRobinGroupChatManager(Agent):
    """Supervisor for a group chat.

    This class coordinates the interactions between agents by invoking
    the next agent in the chat. This manager also uses the parallel
    capabilities of academy to supervize the chat, deciding when to stop,
    but also if, at any point, the conversation has gotten stuck.
    If the conversation is stuck, the manager has the ability to terminate
    the conversation or to modify the running agents to help the
    conversation.

    This implementation could be modified to send messages when the
    conversation gets stuck.

    Args:
        participants: List of people in the chat.
        llm: Langchain LLM to decide stopping condition and supervision
        max_rounds: Number of rounds before chat is terminated by default
        stopping_prompt: Prompt to determine stopping condition.
        supervisor_prompt: Prompt to determine stuck condition
        edit_prompt: Prompt to edit agent.
        supervize_sleep: Time (seconds) between checks of ctuck condition.
            To check every message, set to 0.
    """

    def __init__(  # noqa: PLR0913
        self,
        participants: list[Handle[GroupChatAgent]],
        llm: Any,
        max_rounds: int = 3,
        stopping_prompt: str = DEFAULT_STOPPING_PROMPT,
        supervisor_prompt: str = DEFAULT_SUPERVISOR_PROMPT,
        edit_prompt: str = DEFAULT_EDIT_PROMPT,
        supervize_sleep: float = 30,
    ):
        self.participants = participants
        self.max_rounds = max_rounds
        self.new_messages = asyncio.Event()
        self.history: list[dict[str, str]] = []
        self.running = asyncio.Event()
        self.running.set()
        self.terminate = asyncio.Event()

        self.llm = llm
        self.stopping_prompt = stopping_prompt
        self.supervisor_prompt = supervisor_prompt
        self.edit_prompt = edit_prompt
        self.supervize_sleep = supervize_sleep

    async def agent_on_startup(self) -> None:
        for participant in self.participants:
            for peer in self.participants:
                if peer != participant:
                    await participant.add_peer(peer)

            await participant.add_peer(Handle(self.agent_id))

    @action
    async def receive(self, role: str, content: str) -> None:
        """Add a message to the conversation history.

        Args:
            role: Who in the group chat is sending the message.
            content: Contents of the message.
        """
        logger.info(f'{role} says: {content}')
        self.history.append(
            {
                'role': role,
                'content': content,
            },
        )
        self.new_messages.set()

    def _format_history(self, history: list[dict[str, str]]) -> str:
        return '\n'.join(
            f'{message["role"]}: {message["content"]}' for message in history
        )

    def _check_match(self, response: str, target: str) -> bool:
        return (
            response.lower().strip(string.punctuation).endswith(target.lower())
        )

    @action
    async def query(
        self,
        request: str,
        return_history: bool = False,
    ) -> str | list[dict[str, str]]:
        """Query the agentic system.

        Args:
            request: Question of prompt to start the conversation.
            return_history: Weather to return the entire conversation
                of just the last prompt.

        Returns
           The last message or the entire conversation.
        """
        logger.info(f'User says: {request}')
        for participant in self.participants:
            await participant.receive('user', request)

        remaining_rounds = self.max_rounds
        last_message: str
        while remaining_rounds > 0:
            for participant in self.participants:
                await self.running.wait()
                if self.terminate.is_set():
                    logger.info('Conversation terminated by supervisor.')
                    last_message = 'Conversation terminated by supervisor.'
                    self.history.append(
                        {'role': 'system', 'content': last_message},
                    )
                    self.terminate.clear()
                    break

                last_message = await participant.respond()

            stop_response = await self.llm.ainvoke(
                [
                    {
                        'role': 'system',
                        'content': self.stopping_prompt.format(
                            conversation_history=self._format_history(
                                self.history,
                            ),
                        ),
                    },
                ],
            )
            if self._check_match(stop_response.content, 'STOP'):
                logger.info('Conversation met stopping criteria.')
                break

            remaining_rounds -= 1
        else:
            last_message = 'Conversation reached maximum rounds.'
            logger.info('Conversation reached the maximum number of rounds.')
            self.history.append({'role': 'system', 'content': last_message})

        if return_history:
            return self.history
        return last_message

    @action
    async def clear(self) -> None:
        """Clear the conversation history."""
        self.history = []
        for participant in self.participants:
            await participant.clear()

    @action
    async def update_participant(
        self,
        history: list[dict[str, str]],
        participant: Handle[GroupChatAgent],
    ) -> None:
        """Decide weather the participant needs to be updated.

        Args:
            history: Chat history to base decision.
            participant: Which agent to investigate.
        """
        role = await participant.get_role()
        prompt = await participant.get_prompt()
        history_str = self._format_history(history)
        edit_response = await self.llm.ainvoke(
            [
                {
                    'role': 'system',
                    'content': self.edit_prompt.format(
                        conversation_history=history_str,
                        role=role,
                        prompt=prompt,
                    ),
                },
            ],
        )

        if self._check_match(edit_response.content, 'CONTINUE'):
            return

        start = edit_response.content.rfind('### Prompt:')
        if start > 0:
            new_prompt = edit_response.content[start + len('### Prompt:') :]
            await participant.update_prompt(new_prompt)

        # TODO: Deal with improperly formatted responses
        return

    @loop
    async def supervize(self, shutdown: asyncio.Event) -> None:
        """Background loop to make sure agents are making progress."""

        while not shutdown.is_set():
            await self.new_messages.wait()
            logger.info('Beginning supervior checks.')
            history = list(self.history)
            self.new_messages.clear()

            history_str = self._format_history(history)
            # Perform supervisory actions!
            supervize_response = await self.llm.ainvoke(
                [
                    {
                        'role': 'system',
                        'content': self.supervisor_prompt.format(
                            conversation_history=history_str,
                        ),
                    },
                ],
            )
            logger.info(f'Supervisor Response: {supervize_response.content}')
            if self._check_match(supervize_response.content, 'TERMINATE'):
                self.terminate.set()
                continue

            if self._check_match(supervize_response.content, 'PAUSE'):
                # Pause conversation
                self.running.clear()
                for participant in self.participants:
                    await self.update_participant(history, participant)
                self.running.set()
