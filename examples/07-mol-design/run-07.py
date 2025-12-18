from __future__ import annotations

import asyncio
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from io import StringIO
from typing import Any
from typing import Literal

import numpy as np
from ase.io import read
from ase.optimize import LBFGSLineSearch
from langchain.messages import HumanMessage
from langchain.messages import SystemMessage
from langchain.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.graph import END
from langgraph.graph import START
from langgraph.graph import StateGraph
from rdkit import Chem
from rdkit.Chem import AllChem
from xtb.ase.calculator import XTB

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.exchange import RedisExchangeFactory
from academy.logging import init_logging
from academy.manager import Manager

logger = logging.getLogger(__name__)


def _generate_initial_xyz(mol_string: str) -> str:
    """Generate the XYZ coordinates for a molecule.

    Args:
        mol_string: SMILES string

    Returns:
        - InChI string for the molecule
        - XYZ coordinates for the molecule
    """

    # Generate 3D coordinates for the molecule
    mol = Chem.MolFromSmiles(mol_string)
    if mol is None:
        raise ValueError(f'Parse failure for {mol_string}')
    mol = Chem.AddHs(mol)
    AllChem.EmbedMolecule(mol, randomSeed=1)
    AllChem.MMFFOptimizeMolecule(mol)

    # Save geometry as 3D coordinates
    xyz = f'{mol.GetNumAtoms()}\n'
    xyz += mol_string + '\n'
    conf = mol.GetConformer()
    for i, a in enumerate(mol.GetAtoms()):
        s = a.GetSymbol()
        c = conf.GetAtomPosition(i)
        xyz += f'{s} {c[0]} {c[1]} {c[2]}\n'

    return xyz


@dataclass
class XTBConfig:
    accuracy: float = 0.05
    search_fmax: float = 0.02
    search_steps: int = 250


def _compute_energy_sync(config: XTBConfig, smiles: str) -> float:
    """Run the ionization potential computation."""

    # Make the initial geometry
    xyz = _generate_initial_xyz(smiles)

    # Make the XTB calculator
    calc = XTB(accuracy=config.accuracy)

    # Parse the molecule
    atoms = read(StringIO(xyz), format='xyz')

    # Compute the neutral geometry
    # Uses QCEngine (https://github.com/MolSSI/QCEngine)
    # to handle interfaces to XTB
    atoms.calc = calc
    dyn = LBFGSLineSearch(atoms, logfile=None)
    dyn.run(fmax=config.search_fmax, steps=config.search_steps)

    neutral_energy = atoms.get_potential_energy()

    # Compute the energy of the relaxed geometry in charged form
    charges = np.ones((len(atoms),)) * (1 / len(atoms))
    atoms.set_initial_charges(charges)
    charged_energy = atoms.get_potential_energy()

    return charged_energy - neutral_energy


@dataclass
class SearchState:
    seed: str
    plan: str = ''
    tool_calls: list[Any] = field(default_factory=list)
    simulated_molecules: dict[str, float] = field(default_factory=dict)
    conclusions: list[str] = field(default_factory=list)
    critique: str = ''


class XTBSimulationAgent(Agent):
    """Agent for running XTB to characterize molecules."""

    def __init__(
        self,
        xtb_config: XTBConfig,
        start_molecule: str,
        reasoning_model: str = 'openai/gpt-oss-120b',
        generation_model: str = 'meta-llama/Llama-4-Scout-17B-16E-Instruct',
    ):
        self.config = xtb_config
        self.reasoning_model = reasoning_model
        self.generation_model = generation_model
        self.search_state = SearchState(seed=start_molecule)
        self.molecule_cache: dict[str, float] = {}

    async def agent_on_startup(self) -> None:
        n_workers: int
        if sys.platform == 'darwin':  # pragma: no cover
            n_workers = 2
        else:
            n_workers = max(
                len(os.sched_getaffinity(0)) - 1,
                1,
            )  # Get cores we are assigned to
        self.pool = ProcessPoolExecutor(max_workers=n_workers)

        tools = [tool(self.compute_ionization_energy)]
        self.reasoning_llm = ChatOpenAI(model=self.reasoning_model)
        self.generation_llm = ChatOpenAI(model=self.generation_model)
        self.tools_by_name = {tool.name: tool for tool in tools}
        self.llm_with_tools = self.generation_llm.bind_tools(tools)

    async def agent_on_shutdown(self) -> None:
        self.pool.shutdown()

    async def compute_ionization_energy(self, smiles: str) -> float:
        """Compute the ionization energy for the given molecule.

        Args:
            smiles: SMILES string to evaluate
        Returns:
            Ionization energy in Ha
        """
        if smiles in self.molecule_cache:
            return self.molecule_cache[smiles]

        loop = asyncio.get_event_loop()
        ionization_energy = await loop.run_in_executor(
            self.pool,
            _compute_energy_sync,
            self.config,
            smiles,
        )
        self.molecule_cache[smiles] = ionization_energy
        return ionization_energy

    @loop
    async def conduct_simulation_campaign(  # noqa: C901,PLR0915
        self,
        shutdown: asyncio.Event,
    ) -> None:
        """Conduct a simulation campaign.

        This loop uses an LLM to conduct a search for molecules with high
        ionizationenergy, using the starting molecule as a seed for the
        search space.
        """

        async def plan(state: SearchState) -> SearchState:
            response = await self.reasoning_llm.ainvoke(
                [
                    SystemMessage(
                        content=(
                            'You are a expert computational chemist tasked '
                            'with finding molecules with desired properties. '
                            'You should use simulations to explore the '
                            'chemical space in search of better molecules. '
                            'Come up with a plan for novel molecules to '
                            'simulate. Ground your proposed molecules in '
                            'knowledge of chemical and physical properties. '
                            'For each proposed molecule that you simulate, '
                            'provide the reasoning about why you expect this '
                            'molecule to be better either than previous '
                            'molecules that you have seen, or what you hope '
                            'to learn from running the simulation. In addition'
                            ' to the target property, you should always '
                            'consider things like cost and sythesizability. '
                            'Note: the only experiment your lab is capable of '
                            'running is calculating the ionization energy of '
                            'proposed molecules.',
                        ),
                    ),
                    HumanMessage(
                        content=(
                            'Look for molecules with high ionization energy '
                            'starting with the molecule {state.seed}',
                        ),
                    ),
                ],
            )

            logger.info(f'Planner: {response}')

            return SearchState(
                seed=state.seed,
                plan=response.content,
                tool_calls=state.tool_calls,
                simulated_molecules=state.simulated_molecules,
                conclusions=state.conclusions,
                critique=state.critique,
            )

        async def tool_calling(state: SearchState) -> SearchState:
            previously_simulated = ' '.join(
                state.simulated_molecules.keys(),
            )
            response = await self.llm_with_tools.ainvoke(
                [
                    SystemMessage(
                        content=(
                            'You are a computational chemist who is an expert '
                            'at conducting simulations. Using proposed plan '
                            'and chemical reasoning provided, decide which '
                            'simulations to run using the given tool. Do not '
                            'write python code. You should run several '
                            'simulations at once. Do not repeat previously '
                            'conducted simulations.',
                        ),
                    ),
                    HumanMessage(
                        content=(
                            f'Plan: {state.plan}\n\n'
                            f'Simulated Molecules: {previously_simulated}'
                        ),
                    ),
                ],
            )
            logger.info(f'Tool Calling: {response}')
            while len(response.tool_calls) == 0:
                logger.warning('Tool calling agent did not call tools!')
                logger.warning(response)
                response = await self.llm_with_tools.ainvoke(
                    [
                        SystemMessage(
                            content=(
                                'You are a computational chemist who is '
                                'an expert at conducting simulations. '
                                'sing proposed plan and chemical reasoning'
                                ' provided, decide which simulations to run'
                                ' using the given tool. Do not write python '
                                'code. You should run several simulations at '
                                'once. Do not repeat previously conducted '
                                'simulations.',
                            ),
                        ),
                        HumanMessage(
                            content=(
                                f'Plan: {state.plan}\n\n'
                                f'Simulated Molecules: {previously_simulated}'
                            ),
                        ),
                        response,
                        HumanMessage(
                            content=(
                                'That response does not contain any correctly'
                                'formatted tool calls that could be parsed. '
                                'Please try again! Do not write python code!',
                            ),
                        ),
                    ],
                )

            return SearchState(
                seed=state.seed,
                plan=state.plan,
                tool_calls=response.tool_calls,
                simulated_molecules=state.simulated_molecules,
                conclusions=state.conclusions,
                critique=state.critique,
            )

        async def simulate(state: SearchState) -> SearchState:
            """Performs the tool call"""
            logger.warning(
                f'Number of simulations: {len(state.simulated_molecules)}',
            )
            logger.warning(f'Number of simulations: {len(state.tool_calls)}')
            if len(state.tool_calls) == 0:
                return state

            observations = []
            for tool_call in state.tool_calls:
                tool = self.tools_by_name[tool_call['name']]
                observations.append(
                    asyncio.create_task(tool.ainvoke(tool_call['args'])),
                )

            done, _ = await asyncio.wait(observations)

            results = {}
            for tool_result in zip(state.tool_calls, done, strict=True):
                tool_call, observation = tool_result
                try:
                    results[tool_call['args']['smiles']] = await observation
                except Exception as e:
                    results[tool_call['args']['smiles']] = str(e)

            new_results = state.simulated_molecules | results

            return SearchState(
                seed=state.seed,
                plan=state.plan,
                tool_calls=[],
                simulated_molecules=new_results,
                conclusions=state.conclusions,
                critique=state.critique,
            )

        async def conclude(state: SearchState) -> SearchState:
            results_str = '\n'.join(
                f'{molecule}\t\t{energy}'
                for molecule, energy in state.simulated_molecules.items()
            )
            response = await self.generation_llm.ainvoke(
                [
                    SystemMessage(
                        content=(
                            'You are expert chemist analyzing the results'
                            'of simulations. Synthesize any conclusions from'
                            ' the planned computational campaign and the '
                            'simulation results so far. A -inf result means '
                            'the simulation could not be run, likely because '
                            'the SMILES string of the molecule was invalid. '
                            'You should not repeat conclusions that have '
                            'already been reached but should correct the '
                            'conclusions if they are wrong. For each '
                            'conclusion, output a concise, stand-alone '
                            'statement. Return only the new conclusions.',
                        ),
                    ),
                    HumanMessage(
                        content=(
                            f'Plan: {state.plan}\n\n'
                            'Simulation Results:\n'
                            'Molecule\t\tEnergy\n'
                            f'{results_str}'
                        ),
                    ),
                ],
            )
            logger.info(f'Conclusions: {response}')
            new_state = SearchState(
                seed=state.seed,
                plan=response.content,
                tool_calls=state.tool_calls,
                simulated_molecules=state.simulated_molecules,
                conclusions=[*state.conclusions, response.content],
                critique=state.critique,
            )
            self.search_state = new_state
            return new_state

        async def critique(state: SearchState) -> SearchState:
            results_str = '\n'.join(
                f'{molecule}\t\t{energy}'
                for molecule, energy in state.simulated_molecules.items()
            )
            conclusions = '\n\t'.join(state.conclusions)
            response = await self.generation_llm.ainvoke(
                [
                    SystemMessage(
                        content=(
                            'You are expert chemist analyzing the results of '
                            'simulations. Critique the planned simulation '
                            'campaign based on the chemistry foundations, '
                            'the computational results, and the conclusions '
                            'drawn. Be extremely critical. Look for flaws in '
                            'the methodology and reasoning.',
                        ),
                    ),
                    HumanMessage(
                        content=(
                            f'Plan: {state.plan}\n\n'
                            'Simulation Results:\n'
                            'Molecule\t\tEnergy\n'
                            f'{results_str}\n\n'
                            'Conclusions:\n'
                            f'\t{conclusions}'
                        ),
                    ),
                ],
            )
            logger.info(f'Critique: {response}')
            return SearchState(
                seed=state.seed,
                plan=state.plan,
                tool_calls=state.tool_calls,
                simulated_molecules=state.simulated_molecules,
                conclusions=state.conclusions,
                critique=response.content,
            )

        async def update(state: SearchState) -> SearchState:
            results_str = '\n'.join(
                f'{molecule}\t\t{energy}'
                for molecule, energy in state.simulated_molecules.items()
            )
            conclusions = '\n\t'.join(state.conclusions)
            response = await self.reasoning_llm.ainvoke(
                [
                    SystemMessage(
                        content=(
                            'You are expert chemist analyzing the results of '
                            'simulations. Update the given plan based on the '
                            'results so far, the conclusions drawn and the '
                            'critique given. Ground your updates in current '
                            'results, but also knowledge of chemistry and '
                            'physics. You should output a new, stand-alone '
                            'plan that does not make reference to the '
                            'existing plan. If all of the experiments '
                            'suggested by the plan have been carried out, '
                            'suggest more experiments. Note: the only '
                            'experiment your lab is capable of running is '
                            'calculating the ionization energy of proposed '
                            'molecules.',
                        ),
                    ),
                    HumanMessage(
                        content=(
                            f'Plan: {state.plan}\n\n'
                            'Simulation Results:\n'
                            'Molecule\t\tEnergy\n'
                            f'{results_str}\n\n'
                            'Conclusions:\n'
                            f'\t{conclusions}\n\n'
                            'Critique:\n'
                            f'\t{state.critique}'
                        ),
                    ),
                ],
            )
            logger.info(f'Update: {response}')

            return SearchState(
                seed=state.seed,
                plan=response.content,
                tool_calls=state.tool_calls,
                simulated_molecules=state.simulated_molecules,
                conclusions=state.conclusions,
                critique=state.critique,
            )

        async def should_continue(
            state: SearchState,
        ) -> Literal['critique', END]:  # type: ignore[valid-type]
            if not shutdown.is_set():
                return 'critique'
            return END

        # Build workflow
        agent_builder = StateGraph(SearchState)

        # Add nodes
        agent_builder.add_node('plan', plan)
        agent_builder.add_node('tool_calling', tool_calling)
        agent_builder.add_node('simulate', simulate)
        agent_builder.add_node('conclude', conclude)
        agent_builder.add_node('critique', critique)
        agent_builder.add_node('update', update)

        # Add edges to connect nodes
        agent_builder.add_edge(START, 'plan')
        agent_builder.add_edge('plan', 'tool_calling')
        agent_builder.add_edge('tool_calling', 'simulate')
        agent_builder.add_edge('simulate', 'conclude')
        agent_builder.add_conditional_edges(
            'conclude',
            should_continue,
            ['critique', END],
        )
        agent_builder.add_edge('critique', 'update')
        agent_builder.add_edge('update', 'tool_calling')

        # Compile the agent
        agent = agent_builder.compile()

        # Run until the agent is terminated
        await agent.ainvoke(self.search_state, {'recursion_limit': 10000})

    @action
    async def report(self) -> list[tuple[str, float]]:
        """Summarize findings so far."""
        best_molecules = sorted(
            self.molecule_cache.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return best_molecules


async def main() -> int:
    init_logging(logging.WARNING)

    # config = Config(
    #     executors=[
    #         HighThroughputExecutor(),
    #     ]
    # )
    # executor = ParslPoolExecutor(config)

    async with await Manager.from_exchange_factory(
        factory=RedisExchangeFactory('localhost', 6379),
    ) as manager:
        seeds = [
            'CNC(N)=O',
            # "CC1=C(O)N=C(O)N1",
            # "OC1C2CC3CC1C2O3",
            # "OCC1(CC#N)CCC1"
        ]

        agents = []
        for molecule in seeds:
            agents.append(
                await manager.launch(
                    XTBSimulationAgent,
                    args=(XTBConfig(), molecule),
                ),
            )

        print('Starting discovery campaign')
        print('=' * 80)
        while True:
            await asyncio.sleep(30)
            for i, agent in enumerate(agents):
                print(f'Progress report from agent {i}')
                report = await agent.report()
                print(f'Number of molecules simulated: {len(report)}')
                print(f'Five best molecules: {report[:5]}')
            print('=' * 80)

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
