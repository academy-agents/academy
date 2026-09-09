from __future__ import annotations

import contextlib
import os
import pathlib
import random
import signal
import subprocess
import time

import z3

# turn on here_mode if you want the driver to only test combinations which
# involve the HERE version - for example, this makes sense if you are
# contributing a pull request which does not modify the crossver tests in
# any way.
here_mode = False

dry_run_mode = False

from .helpers import create_env, managed_commandline


def run_test_1(version_set: dict):
    """Test agent, client and HTTP exchange from different environments.


    """
    v1_env = create_env(version_set['exchange'])
    v2_env = create_env(version_set['agent'])
    v3_env = create_env(version_set['client'])


    # This config file was sourced from
    # tests/unit/exchange/cloud/app_test.py
    # at the time of writing.
    # It is hard-coded here because keeping the configuration
    # file compatible across versions is probably desirable.

    exchange_config = """
host = "localhost"
port = 1234
"""

    with open(v1_env / 'exchange_config.json', 'w') as f:
        f.write(exchange_config)

    with managed_commandline(f'python3 -m academy.exchange.cloud.__main__ --config exchange_config.json', daemon=True, env=v1_env) as p1:

        # this sleep and the one a few lines down is to give the daemons
        # enough time to get started. There are probably more reliable and
        # quicker ways to do this.
        time.sleep(1)

        base = os.getcwd()

        with managed_commandline(f'python3 {base}/tests/crossver/test_1/agent.py', daemon=True, env=v2_env) as p2:

            time.sleep(3)

            # This might be the same location, if v2 = v3
            os.system(f'cp {v2_env}/agent.handle {v3_env}/agent.handle')

            with managed_commandline(f'python3 {base}/tests/crossver/test_1/client.py', daemon=False, env=v3_env) as p3:
                pass


def run_test_heartbeat(version_set: dict):

    v1_env = create_env(version_set['exchange'])
    v2_env = create_env(version_set['agent'])
    v3_env = create_env(version_set['client'])

    exchange_config = """
host = "localhost"
port = 1234
"""

    with open(v1_env / 'exchange_config.json', 'w') as f:
        f.write(exchange_config)

    with managed_commandline(f'python3 -m academy.exchange.cloud.__main__ --config exchange_config.json', daemon=True, env=v1_env) as p1:
        time.sleep(1)

        base = os.getcwd()

        with managed_commandline(f'python3 {base}/tests/crossver/test_heartbeat/agent.py', daemon=True, env=v2_env) as p2:

            time.sleep(3)

            os.system(f'cp {v2_env}/agent.handle {v3_env}/agent.handle')

            with managed_commandline(f'python3 {base}/tests/crossver/test_heartbeat/client.py', daemon=False, env=v3_env) as p3:
                pass


def run_test_entity_status_client_0_5_0(version_set: dict):

    v1_env = create_env(version_set['exchange'])
    v2_env = create_env(version_set['agent'])
    v3_env = create_env(version_set['client'])

    exchange_config = """
host = "localhost"
port = 1234
"""

    with open(v1_env / 'exchange_config.json', 'w') as f:
        f.write(exchange_config)

    with managed_commandline(f'python3 -m academy.exchange.cloud.__main__ --config exchange_config.json', daemon=True, env=v1_env) as p1:

        time.sleep(1)

        base = os.getcwd()

        with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_0_5_0/agent.py', daemon=True, env=v2_env) as p2:

            time.sleep(3)

            os.system(f'cp {v2_env}/agent.handle {v3_env}/agent.handle')

            with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_0_5_0/client.py', daemon=False, env=v3_env) as p3:
                pass


def run_test_entity_status_client_1_0_0(version_set: dict):

    v1_env = create_env(version_set['exchange'])
    v2_env = create_env(version_set['agent'])
    v3_env = create_env(version_set['client'])

    exchange_config = """
host = "localhost"
port = 1234
"""

    with open(v1_env / 'exchange_config.json', 'w') as f:
        f.write(exchange_config)

    with managed_commandline(f'python3 -m academy.exchange.cloud.__main__ --config exchange_config.json', daemon=True, env=v1_env) as p1:

        time.sleep(1)

        base = os.getcwd()

        with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_1_0_0/agent.py', daemon=True, env=v2_env) as p2:

            time.sleep(3)

            os.system(f'cp {v2_env}/agent.handle {v3_env}/agent.handle')

            with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_1_0_0/client.py', daemon=False, env=v3_env) as p3:
                pass


def run_test_api_thread_executor_logconfig(version_set: dict):

    v1_env = create_env(version_set['program'])

    base = os.getcwd()

    with managed_commandline(f'python3 {base}/tests/crossver/test_api_thread_executor_logconfig/clientagent.py', daemon=False, env=v1_env):
        pass


def run_test_api_thread_executor_nolog(version_set: dict):

    v1_env = create_env(version_set['program'])

    base = os.getcwd()

    with managed_commandline(f'python3 {base}/tests/crossver/test_api_thread_executor_nolog/clientagent.py', daemon=False, env=v1_env):
        pass


def run_test_pickle_handle(version_set: dict):
    v1_env = create_env(version_set['writer'])
    v2_env = create_env(version_set['reader'])
    base = os.getcwd()

    # these two could be the same file
    os.system(f'rm -f {v1_env}/pickle.handle')
    os.system(f'rm -f {v2_env}/pickle.handle')
    with managed_commandline(f'python3 {base}/tests/crossver/test_pickle_handle/serializer.py', daemon=False, env=v1_env):
        pass

    os.system(f'cp {v1_env}/pickle.handle {v2_env}/pickle.handle')

    with managed_commandline(f'python3 {base}/tests/crossver/test_pickle_handle/deserializer.py', daemon=False, env=v2_env):
        pass


AcademyVersion, (v030, v031, v040, v050, v060, v070, v100, v_here) = z3.EnumSort(
    'AcademyVersion',
    [
        'academy-py==0.3.0',
        'academy-py==0.3.1',
        'academy-py==0.4.0',
        'packaging academy-py==0.5.0',
        'packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1',
        'packaging git+https://github.com/academy-agents/academy@2c2127324aacf5e6402b665876b9e7e548c9506d',
        'academy-py==1.0.0',
        'HERE',
    ],
)

v1 = z3.Const('v1', AcademyVersion)
v2 = z3.Const('v2', AcademyVersion)
v3 = z3.Const('v3', AcademyVersion)

# Three-environment tests (exchange, agent, client)


if here_mode:
    solver.add(z3.Or(v1 == v_here, v2 == v_here, v3 == v_here))

# simulations of semver...

def post_040(v):
    return z3.Or(v == v040, v == v050, v == v060, v == v070, v == v100, v == v_here)

def post_050(v):
    # speaks the post-050 protocol
    return z3.Or(           v == v050, v == v060, v == v070, v == v100, v == v_here)

def post_060(v):
    # "fake" version
    # dff0 is the "pre-release" of heartbeats, before 1.0.0
    return z3.Or(post_100(v), v == v060, v == v070)

def pre_070(v):
    return z3.Not(post_070(v))

def post_070(v):
    return z3.Or(post_100(v), v == v070)

def post_100(v):
    return z3.Or(                                                  v == v100, v == v_here)

def pre_100(v):
    return z3.Or(v == v030, v == v031, v == v040, v == v050)


solver = z3.Solver()

# If either the client or agent has heartbeats implemented, then
# the HTTP Exchange needs to support heartbeats.
# From a semver perspective, this is a major-version change,
# but this implication can express that more subtly.
solver.add(z3.Implies(post_060(v2), post_060(v1)))
solver.add(z3.Implies(post_060(v3), post_060(v1)))


# The HTTP Exchange wire protocol changed incompatibly from 0.4.0 to 0.5.0
# so if any component is past 0.5.0 then they must all be that way.
# The 0.5.0 protocol should still work with academy version 1.0.0 etc
# so there is no upper bound here.
solver.add(z3.Implies(post_050(v1), post_050(v2)))
solver.add(z3.Implies(post_050(v2), post_050(v3)))
solver.add(z3.Implies(post_050(v3), post_050(v1)))


solver.push()

solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_1: solution {count} ===')
    print(m)
    # when a v is not bound, force a choice. it doesn't matter what.
    chosen_v1 = m[v1] if m[v1] is not None else v040
    chosen_v2 = m[v2] if m[v2] is not None else v040
    chosen_v3 = m[v3] if m[v3] is not None else v040
    solver.add(
        z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)),
    )

    _V1 = {'academy': str(chosen_v1)}
    _V2 = {'academy': str(chosen_v2)}
    _V3 = {'academy': str(chosen_v3)}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_1(this_version_set)

# pops the iteration-forcing constraints and anything that is specific
# to particular test case (nothing in this case, but different later).
solver.pop()

# TODO - test these with different constraint sets.

solver.push()

# all the above constraints, plus a constraint that
# the agent definitely has heartbeat support.

solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))
solver.add(post_060(v2))
count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_heartbeat: solution {count} ===')
    print(m)
    # when a v is not bound, force a choice. it doesn't matter what.
    chosen_v1 = m[v1] if m[v1] is not None else v040
    chosen_v2 = m[v2] if m[v2] is not None else v040
    chosen_v3 = m[v3] if m[v3] is not None else v040
    solver.add(
        z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)),
    )

    _V1 = {'academy': str(chosen_v1)}
    _V2 = {'academy': str(chosen_v2)}
    _V3 = {'academy': str(chosen_v3)}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_heartbeat(this_version_set)
solver.pop()

solver.push()

# This is a semver-style lower bound on all components.
solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))

# This is an open upper bound on the client API, not on all
# components. If trying to be purely semver, this would be
# an open upper bound on all components.

# Compatibility breaks at this point because MailboxStatus was moved
# to a new source code location, so it cannot be imported any more by
# this test's client script.

# This isn't a constraint on the wire protocol.
solver.add(pre_070(v3))

# PR #404 switches the client API for status from asking for
# client status in the old way (whatever that was?) to implementing
# a heartbeat-based status. In order for that to work, the agent
# must be new enough to emit heartbeats too.

# But not the other way round: an older client can still see status
# from newer agents (but will have the old status semantics, not
# heartbeat style semantics)

# So pr #404 should/would be a major version increment, but this
# implication describes more subtleties.

solver.add(z3.Implies(post_060(v3), post_060(v2)))

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_entity_status_client_0_5_0: solution {count} ===')
    print(m)
    # when a v is not bound, force a choice. it doesn't matter what.
    chosen_v1 = m[v1] if m[v1] is not None else v040
    chosen_v2 = m[v2] if m[v2] is not None else v040
    chosen_v3 = m[v3] if m[v3] is not None else v040
    solver.add(
        z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)),
    )

    _V1 = {'academy': str(chosen_v1)}
    _V2 = {'academy': str(chosen_v2)}
    _V3 = {'academy': str(chosen_v3)}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_entity_status_client_0_5_0(this_version_set)
solver.pop()


solver.push()

# same wire-protocol constraints at the 0_5_0 test
solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))
solver.add(z3.Implies(post_060(v3), post_060(v2)))

# because of status Python API changes
solver.add(post_070(v3))


count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_entity_status_client_1_0_0: solution {count} ===')
    print(m)
    # when a v is not bound, force a choice. it doesn't matter what.
    chosen_v1 = m[v1] if m[v1] is not None else v040
    chosen_v2 = m[v2] if m[v2] is not None else v040
    chosen_v3 = m[v3] if m[v3] is not None else v040
    solver.add(
        z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)),
    )

    _V1 = {'academy': str(chosen_v1)}
    _V2 = {'academy': str(chosen_v2)}
    _V3 = {'academy': str(chosen_v3)}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_entity_status_client_1_0_0(this_version_set)
solver.pop()


# Two-environment tests (for example, pickle/unpickle handle)

solver = z3.Solver()

if here_mode:
  solver.add(z3.Or(v1 == v_here, v2 == v_here))

solver.push()

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_pickle_handle: solution {count} ===')
    print(m)
    chosen_v1 = m[v1] if m[v1] is not None else v040
    chosen_v2 = m[v2] if m[v2] is not None else v040
    solver.add(
        z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2)),
    )

    _V1 = {'academy': str(chosen_v1)}
    _V2 = {'academy': str(chosen_v2)}
    this_version_set = {'writer': _V1, 'reader': _V2}

    if not dry_run_mode:
        run_test_pickle_handle(this_version_set)

solver.pop()


# One-environment tests (for example, Python API regression tests)

solver = z3.Solver()

if here_mode:
    solver.add(v1 == v_here)

solver.push()

# this test uses logging API changes introduced in v0.5.0
solver.add(post_050(v1))

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_api_thread_executor_logconfig: solution {count} ===')
    print(m)
    chosen_v1 = m[v1] if m[v1] is not None else v040
    solver.add(z3.Not(v1 == chosen_v1))
    _V1 = {'academy': str(chosen_v1)}
    this_version_set = {'program': _V1}

    if not dry_run_mode:
        run_test_api_thread_executor_logconfig(this_version_set)
solver.pop()

solver.push()

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_api_thread_executor_nolog: solution {count} ===')
    print(m)
    chosen_v1 = m[v1] if m[v1] is not None else v040
    solver.add(z3.Not(v1 == chosen_v1))
    _V1 = {'academy': str(chosen_v1)}
    this_version_set = {'program': _V1}

    if not dry_run_mode:
        run_test_api_thread_executor_nolog(this_version_set)
solver.pop()
