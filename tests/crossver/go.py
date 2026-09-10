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

from .helpers import VersionSort, valid_academy_version, create_env, managed_commandline
from .helpers import *


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


v1 = z3.Const('v1', VersionSort)
v2 = z3.Const('v2', VersionSort)
v3 = z3.Const('v3', VersionSort)

# Three-environment tests (exchange, agent, client)


if here_mode:
    solver.add(z3.Or(v1 == v_here, v2 == v_here, v3 == v_here))

# simulations of semver...

# this deliberately omits a patch field, because of semver
# semantics
def closed_minimum_version(v, major, minor):
    if isinstance(v, list):
        return z3.And(*map(lambda v2: closed_minimum_version(v2, major, minor), v))

    return z3.Or(
        z3.And(VersionSort.major(v) == major, VersionSort.minor(v) >= minor),
        z3.And(VersionSort.major(v) > major))


def compatibility_breaks_at(vs, major, minor=None):
    # declares a breakage at specified major version that means that either:
    # all versions are pre the specified major version or
    # all versions are post the major version
    # This only accepts a major version, which means to use this constraint,
    # you are forced to declare a new major version - in alignment with semer.

    if minor is not None:
        assert major == 0, "breakage is only permitted on major versions or dev-era minor versions"
    else:
        minor = 0

    return z3.And(*(z3.Implies(closed_minimum_version(l, major, minor), closed_minimum_version(r, major, minor)) for l, r in zip(vs, vs[1:] + [vs[0]])))


def post_060(v):
    return closed_minimum_version(v, 0,6)

def post_070(v):
    return closed_minimum_version(v, 0,7)

def pre_070(v):
    return z3.Not(post_070(v))

def post_100(v):
    return closed_minimum_version(v, 1,0)

def pre_100(v):
    return z3.Not(post_100(v))


solver = z3.Solver()

solver.add(valid_academy_version(v1))
solver.add(valid_academy_version(v2))
solver.add(valid_academy_version(v3))


# The HTTP Exchange wire protocol changed incompatibly from 0.4.0 to 0.5.0
# so if any component is past 0.5.0 then they must all be that way.
# The 0.5.0 protocol should still work with academy version 1.0.0 etc
# so there is no upper bound here.
# This is a "breaks at major version" declaration:
# the python side API works before the major version change, and after
# the major version change, but only wire/protocol compatible with
# versions before/after
solver.add(compatibility_breaks_at([v1, v2, v3], 0, 5))


# If either the client or agent has heartbeats implemented, then
# the HTTP Exchange needs to support heartbeats.
# From a semver perspective, this is a major-version change,
# but this implication can express that more subtly, to support
# testing that a newer exchange will work with older clients and
# agents.
solver.add(z3.Implies(post_060(v2), post_060(v1)))
solver.add(z3.Implies(post_060(v3), post_060(v1)))

solver.push()

solver.add(closed_minimum_version([v1, v2, v3], 0, 4))

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

    _V1 = {'academy': chosen_v1}
    _V2 = {'academy': chosen_v2}
    _V3 = {'academy': chosen_v3}

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

solver.add(closed_minimum_version([v1, v2, v3], 0, 4))
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

    _V1 = {'academy': chosen_v1}
    _V2 = {'academy': chosen_v2}
    _V3 = {'academy': chosen_v3}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_heartbeat(this_version_set)
solver.pop()

solver.push()

# This is a semver-style lower bound on all components.
solver.add(closed_minimum_version([v1, v2, v3], 0, 4))

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

    _V1 = {'academy': chosen_v1}
    _V2 = {'academy': chosen_v2}
    _V3 = {'academy': chosen_v3}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_entity_status_client_0_5_0(this_version_set)
solver.pop()


solver.push()

# same wire-protocol constraints at the 0_5_0 test

solver.add(closed_minimum_version([v1, v2, v3], 0, 4))
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

    _V1 = {'academy': chosen_v1}
    _V2 = {'academy': chosen_v2}
    _V3 = {'academy': chosen_v3}

    this_version_set = {'exchange': _V1, 'agent': _V2, 'client': _V3}

    if not dry_run_mode:
        run_test_entity_status_client_1_0_0(this_version_set)
solver.pop()


# Two-environment tests (for example, pickle/unpickle handle)

solver = z3.Solver()

solver.add(valid_academy_version(v1))
solver.add(valid_academy_version(v2))

if here_mode:
  solver.add(z3.Or(v1 == v_here, v2 == v_here))

solver.push()

solver.add(closed_minimum_version([v1, v2], 0, 3))

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

    _V1 = {'academy': chosen_v1}
    _V2 = {'academy': chosen_v2}
    this_version_set = {'writer': _V1, 'reader': _V2}

    if not dry_run_mode:
        run_test_pickle_handle(this_version_set)

solver.pop()


# One-environment tests (for example, Python API regression tests)

solver = z3.Solver()

solver.add(valid_academy_version(v1))

if here_mode:
    solver.add(v1 == v_here)

solver.push()

# this test uses logging API changes introduced in v0.5.0
solver.add(closed_minimum_version(v1, 0, 5))

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_api_thread_executor_logconfig: solution {count} ===')
    print(m)
    chosen_v1 = m[v1] if m[v1] is not None else v040
    solver.add(z3.Not(v1 == chosen_v1))
    _V1 = {'academy': chosen_v1}
    this_version_set = {'program': _V1}

    if not dry_run_mode:
        run_test_api_thread_executor_logconfig(this_version_set)
solver.pop()

solver.push()

solver.add(closed_minimum_version(v1, 0, 3))
count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_api_thread_executor_nolog: solution {count} ===')
    print(m)
    chosen_v1 = m[v1] if m[v1] is not None else v040
    solver.add(z3.Not(v1 == chosen_v1))
    _V1 = {'academy': chosen_v1}
    this_version_set = {'program': _V1}

    if not dry_run_mode:
        run_test_api_thread_executor_nolog(this_version_set)
solver.pop()
