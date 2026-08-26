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


envs = {}

test_run_root: pathlib.Path | None = None

def create_env(descr: dict) -> pathlib.Path:
    global test_run_root

    if test_run_root is None:
        test_run_root = pathlib.Path('.') / ('crossver-' + str(random.randint(0, 999999999)))

    assert test_run_root is not None

    if (
        str(descr) in envs
    ):  # this is a bit denormalised so will result in false negatives but not false positives?
        print(f'Using cached environment: {descr!s}')
        return envs[str(descr)]

    env_path = test_run_root / (
        'env_' + str(random.randint(0, 999999999))
    )

    print(f'creating env for {descr} at {env_path}')

    dir = env_path.mkdir(parents=True, exist_ok=True)

    here_path = os.getcwd()

    install_target = descr['academy']
    if install_target == 'HERE':
        install_target = here_path

    os.system(
        f'cd {env_path}; virtualenv ./venv; pwd; ls; . ./venv/bin/activate; which python; pip install {install_target}',
    )

    envs[str(descr)] = env_path
    return env_path


@contextlib.contextmanager
def managed_commandline(cmdline: str, *, daemon: bool, env: str):
    """context manager for managed process execution around a block of code.

    This manager will start up the process on entering the block, and
    ensure it is shut down when leaving the block.

    daemon mode specifies the shutdown expectations which will be enforced
    on leaving the block:

    a non-daemon process is expected to exit successfully itself and the
    managed commandline block will wait when leaving the block until that
    process has exited, and will raise an exception if the exit code is
    not 0.

    a daemon process is expected to remain alive up to the point that
    the block is left. the manager will terminate the process tree, and
    will raise an exception is the process exited by some other mechanism.
    """

    p = subprocess.Popen(
        f"set -e; cd {env}; . venv/bin/activate;" + cmdline,
        shell=True,
        process_group=0,
    )

    try:
        yield p
    finally:
        if daemon:
            print('terminating')
            os.killpg(p.pid, signal.SIGTERM)
            print('waiting on process')
            p.wait()
            assert p.returncode == -15, 'process should have been terminated by SIGTERM'
        else:
            p.wait()
            assert p.returncode == 0, 'process should have exited successfully'


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

    print(
        f'return codes: p1={p1.returncode}, p2={p2.returncode}, p3={p3.returncode}',
    )


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

    print(
        f'return codes: p1={p1.returncode}, p2={p2.returncode}, p3={p3.returncode}',
    )


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


def run_test_entity_status_client_0_6_0(version_set: dict):

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

        with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_0_6_0/agent.py', daemon=True, env=v2_env) as p2:

            time.sleep(3)

            os.system(f'cp {v2_env}/agent.handle {v3_env}/agent.handle')

            with managed_commandline(f'python3 {base}/tests/crossver/test_entity_status_client_0_6_0/client.py', daemon=False, env=v3_env) as p3:
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


AcademyVersion, (v030, v031, v040, v050, v_pr404, v_pr447, v_here) = z3.EnumSort(
    'AcademyVersion',
    [
        'academy-py==0.3.0',
        'academy-py==0.3.1',
        'academy-py==0.4.0',
        'packaging academy-py==0.5.0',
        'packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1',
        'packaging git+https://github.com/academy-agents/academy@2c2127324aacf5e6402b665876b9e7e548c9506d',
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
    return z3.Or(v == v040, v == v050, v == v_pr404, v == v_pr447, v == v_here)

def post_050(v):
    # speaks the post-050 protocol
    return z3.Or(           v == v050, v == v_pr404, v == v_pr447, v == v_here)

def post_060(v):
    return                                                         v == v_here

def pre_060(v):
    return z3.Or(v == v030, v == v031, v == v040, v == v050)

def post_pr404(v):
    # alias for post_060, in the semantic version world
    # but we have some more nuance because v_pr404 is a non-semver-tagged
    # commit that is still interesting to test against so maybe it can
    # be semvered with a negative minor number? or maybe a later one
    # breaks things?
    # dff0 is the "pre-release" of heartbeats, before 0.6.0
    return z3.Or(post_060(v), v == v_pr404, v == v_pr447)

def post_pr447(v):
    return z3.Or(post_060(v), v == v_pr447)


solver = z3.Solver()

# This combination causes test 1 to fail, because the 0.5.0 exchange
# doesn't recognise heartbeats: the agent outputs background error
# (but maybe still functions) and the client fails with a unix error,
# which is actually what is detected.
# [v2 = HERE, v1 = packaging academy-py==0.5.0, v3 = HERE]
# So what is the broader constraint here?
# probably an implication: agent or client being >= dff0
# implies that the exchange must be >= dff0
# that's quite subtle and not something that can be expressed
# in semver in the presence of other similar constraints, I think.

solver.add(z3.Implies(post_pr404(v2), post_pr404(v1)))
solver.add(z3.Implies(post_pr404(v3), post_pr404(v1)))


# This combination fails because the wire protocol for http exchange
# changed from 0.4.0 to 0.5.0
# [v2 = packaging academy-py==0.5.0, v1 = packaging academy-py==0.4.0, v3 = packaging academy-py==0.4.0]
# this is needed for the HTTP exchange protocol which changed
# incompatibly from 0.4.0 to 0.5.0
# but it's only needed for tests that use the HTTP exchange.
# How to represent that?
# "all or none" constraint between multiple versions:
solver.add(z3.Implies(post_050(v1), post_050(v2)))
solver.add(z3.Implies(post_050(v2), post_050(v3)))
solver.add(z3.Implies(post_050(v3), post_050(v1)))
# this will interact with the post_pr404 protocol constraint above too
# which is not bi-directional...


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

    run_test_1(this_version_set)

# pops the iteration-forcing constraints and anything that is specific
# to particular test case (nothing in this case, but different later).
solver.pop()

# TODO - test these with different constraint sets.

solver.push()

# all the above constraints, plus a constraint that
# the agent definitely has heartbeat support.

solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))
solver.add(post_pr404(v2))
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

    run_test_heartbeat(this_version_set)
solver.pop()

solver.push()

# this is a requirement because... using the HTTP exchange?
solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))

# same as the base compatibility rules
# although I'll probably need to add in an exclusion for an undesired incompatibility

# BUG:
# This combination reports that the agent is inactive.
# v1 = git+https://github.com/academy-agents/academy@main]
# v2 = packaging academy-py==0.5.0,
# v3 = packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1,
# Here's a workaround, but it's not entirely desirable: it means, for example,
# 0.5.0 can't talk to 0.6.0 for agent status.

# Is this a wire protocol or API or exchange related constraint?
solver.add(z3.Implies(post_pr404(v3), post_pr404(v2)))

# This test breaks in 0.6.0 post-PR#447 because of movement of
# MailboxStatus structure to different module.
solver.add(pre_060(v2))
solver.add(pre_060(v3))

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

    run_test_entity_status_client_0_5_0(this_version_set)
solver.pop()


solver.push()

# because of http exchange protocol
solver.add(z3.And(post_040(v1), post_040(v2), post_040(v3)))

# because of status Python API changes
solver.add(post_pr447(v3))

# same as the base compatibility rules
# although I'll probably need to add in an exclusion for an undesired incompatibility

# BUG:
# This combination reports that the agent is inactive.
# v1 = git+https://github.com/academy-agents/academy@main]
# v2 = packaging academy-py==0.5.0,
# v3 = packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1,
# Here's a workaround, but it's not entirely desirable: it means, for example,
# 0.5.0 can't talk to 0.6.0 for agent status.
solver.add(z3.Implies(post_pr404(v3), post_pr404(v2)))

count = 0
while solver.check() == z3.sat:
    count += 1
    m = solver.model()
    print(f'=== test_entity_status_client_0_6_0: solution {count} ===')
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

    run_test_entity_status_client_0_6_0(this_version_set)
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
    run_test_api_thread_executor_nolog(this_version_set)
solver.pop()
