import random

import os
import pathlib
import signal
import subprocess
import time

# this can run in a fairly arbitrary python

# There should be several versions available.
# Actually the number is unbounded because tests might want to be able
# to start up arbitrary other components of arbitrary version... but
# for now, a client-ish, and exchange-ish, and an agent-ish.

# these can be chosen in various combos: earliest that matches the version constraint,
# most recent, "pr current", random from main branch, random release that matches (a subset of random-from-main)

# these envs can also contain other version constrains eventaully, not only academy version.
# (python version, redis version, ...). python version is probably especially useful for being able to
# deal with peoples marginal ideas on cross-python compatibility.

# The notion of a current/here version is special though because that's where this driver script and the tests
# live (probably) - and might exist only as a current filesystem artefact, unlike the other globally named
# versions.

# It makes sense to run the current version against a *later* version (as part of testing that later version)
# which itself is a bit nuanced -- I want to know that what i am writing now is not broken by future changes.

# It's also possible I want to test API consistency, not just wire protocol consistency? what does that look
# like? Test cases from version x, but run against version y (so that if someone edits the test cases to make
# version y work, we discover than when running the version x tests)?  Or is that covered by "do not edit these
# tests without an API bump or untested-justification of how this isn't breaking things?"
# Testing this API compatibility is probably interesting/useful though.
# And at least good for modelling what version meanings are in that space, separate from wire protocols.
# There will be testing of that a bit though -- we'll be testing that the Python test code from HERE will run
# in an environment where we've got V1, or V2, or V3 installed.

# Config file format is also a thing: if i generate a config file now, does it work across older and newer
# versions? - for example, the config file for the http exchange. if I generate it "now" (aka from this current
# version of the test suite), will it be accepted by future and past implementations of the http exchange?

# are all these versioned by the same version (the pypi package version) or is there more interesting stuff
# going on that users (and academy code) needs to understand?

def create_env(descr: dict) -> pathlib.Path:
    env_path = pathlib.Path(".") / ("crossver-env-" + str(random.randint(0,999999999)) + descr['name'])

    print(f"creating env for {descr} at {env_path}")

    dir = env_path.mkdir()

    here_path = os.getcwd()


    install_target = descr['academy']
    if install_target == "HERE":
        install_target = here_path

    os.system(f"cd {env_path}; virtualenv ./venv; pwd; ls; . ./venv/bin/activate; which python; pip install {install_target}")

    return env_path


def run_test_1(version_set: dict):
  # given where we are now, (aka HERE and CURRENT_ERA),
  # what academy versions should be compatible?

  # HERE is always compatible

  # there might be one or more academy releases that are
  # compatible, and installable from pypi to validate that
  # packaged path.

  # there might be one or more git commits that are compatible.
  # some probably interesting ones are:
  #  the commit that changed the current era to its current value
  #  the most recent main
  #  the most recent merge based of HEAD and main
  # For inspecting these, it probably makes sense to move current era into
  # its own non-python file, so that git commands can dump it out without
  # doing a whole checkout (or doing a checkout at all, with suitable
  # git magic)

  # basic test model is:
  # a submitter/client
  # an exchange process
  # an agent process

  # we are going to test that we can do a ping to the agent.

  # In this level, we should not be expecting any academy to be installed
  # because there's no meaningful version to be active - there are 3. Anything
  # academy-like has to happen in the relevant environments.

  for k, v in version_set.items():
    v['name'] = k

  v1_env = create_env(version_set['exchange'])
  v2_env = create_env(version_set['agent'])
  v3_env = create_env(version_set['client'])

  # now i need something like an integration test, but clearly split into three environments...
  # and to separately and concurrently run the three components.
  # (rather than the in-python test fixture starting up an agent inside the same Python function
  # as the client)


  # first startup an exchange
  # type of exchange could be parametrizable but it needs to be at least cross process accessible - 
  # so redis and http?
  # a redis exchange has no active process and no active code - it's a shared redis instance. so it
  # wont' need its own environment to be installed.

  # V1 is the http exchange environment

  # the config file is generated by HERE, not by the V1 environment,
  # because I expect a config file for the http exchange to be compatible across versions.
  # This config was originally sourced from tests/unit/exchange/cloud/app_test.py 


  exchange_config = """
host = "localhost"
port = 1234
"""

  with open(v1_env / "exchange_config.json", "w") as f:
    f.write(exchange_config)


  # now run some kind of process group that is async wrt rest of program and can be shut down
  # entirely at the end (not just the process but all children)

  p1 = subprocess.Popen(f"set -ex; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  # a bit of startup time... probs could be done by probing?
  import time
  time.sleep(5)

  base = os.getcwd()

  # V2 should be an agent (against the major-version-consistent API, with no new minor version features)
  # run in the V2 environment.
  p2 = subprocess.Popen(f"set -ex; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_1/agent.py", shell=True, process_group=0)

  time.sleep(10)

  # need some time ^ for the agent to get started and write out its agent handle file

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -ex; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_1/client.py", shell=True, process_group=0)

  # p3 should exit when tests finished -- no need to terminate it, or have a time based wait.
  p3.wait()

  print("terminating p2 group")
  os.killpg(p2.pid, signal.SIGTERM)
  print("waiting on p2")
  p2.wait()


  print("terminating p1 group")
  os.killpg(p1.pid, signal.SIGTERM)

  print("waiting on p1")
  p1.wait()

  print(f"return codes: p1={p1.returncode}, p2={p2.returncode}, p3={p3.returncode}")

  assert p1.returncode == -15, "p1 should have been terminated by SIGTERM"
  assert p2.returncode == -15, "p2 should have been terminated by SIGTERM"
  assert p3.returncode == 0, "p3 should have exited succesfully"



def run_test_heartbeat(version_set: dict):
  for k, v in version_set.items():
    v['name'] = k

  v1_env = create_env(version_set['exchange'])
  v2_env = create_env(version_set['agent'])
  v3_env = create_env(version_set['client'])

  exchange_config = """
host = "localhost"
port = 1234
"""

  with open(v1_env / "exchange_config.json", "w") as f:
    f.write(exchange_config)

  p1 = subprocess.Popen(f"set -ex; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  import time
  time.sleep(5)

  base = os.getcwd()

  p2 = subprocess.Popen(f"set -ex; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_heartbeat/agent.py", shell=True, process_group=0)

  time.sleep(10)

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -ex; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_heartbeat/client.py", shell=True, process_group=0)

  p3.wait()

  print("terminating p2 group")
  os.killpg(p2.pid, signal.SIGTERM)
  print("waiting on p2")
  p2.wait()


  print("terminating p1 group")
  os.killpg(p1.pid, signal.SIGTERM)

  print("waiting on p1")
  p1.wait()

  print(f"return codes: p1={p1.returncode}, p2={p2.returncode}, p3={p3.returncode}")

  assert p1.returncode == -15, "p1 should have been terminated by SIGTERM"
  assert p2.returncode == -15, "p2 should have been terminated by SIGTERM"
  assert p3.returncode == 0, "p3 should have exited succesfully"


def run_test_entity_status_client(version_set: dict):
  for k, v in version_set.items():
    v['name'] = k

  v1_env = create_env(version_set['exchange'])
  v2_env = create_env(version_set['agent'])
  v3_env = create_env(version_set['client'])

  exchange_config = """
host = "localhost"
port = 1234
"""

  with open(v1_env / "exchange_config.json", "w") as f:
    f.write(exchange_config)

  p1 = subprocess.Popen(f"set -ex; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  import time
  time.sleep(5)

  base = os.getcwd()

  p2 = subprocess.Popen(f"set -ex; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_entity_status_client/agent.py", shell=True, process_group=0)

  time.sleep(10)

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -ex; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_entity_status_client/client.py", shell=True, process_group=0)

  p3.wait()

  print("terminating p2 group")
  os.killpg(p2.pid, signal.SIGTERM)
  print("waiting on p2")
  p2.wait()


  print("terminating p1 group")
  os.killpg(p1.pid, signal.SIGTERM)

  print("waiting on p1")
  p1.wait()

  print(f"return codes: p1={p1.returncode}, p2={p2.returncode}, p3={p3.returncode}")

  assert p1.returncode == -15, "p1 should have been terminated by SIGTERM"
  assert p2.returncode == -15, "p2 should have been terminated by SIGTERM"
  assert p3.returncode == 0, "p3 should have exited succesfully"


# although 0.5.0 is outside the current-era, this is testing that code written
# to the HERE API will also run against 0.5.0, which is a different style of
# era that is maybe still worth testing - it's a Python API era.
# so maybe develop the eras code with an eye to expecting there to be two or
# more era parameters/concepts?
# the testing for API eras is different to testing HERE tests vs three different
# in-current-era package versions.


# unfolded constraint: all three component versions are identical, version >= 0.4.0
test1_samevers = ["HERE",
                  "packaging git+https://github.com/academy-agents/academy@main",
                  "packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1",
                  "packaging academy-py==0.5.0",
                  "packaging academy-py==0.4.0"
                  ]

# test_1 doesn't work against all 0.3.0, because:
#   TypeError: Manager.from_exchange_factory() missing 1 required positional argument: 'executors'
for test1_samever in test1_samevers:

  _V1={"academy": test1_samever}
  _V2={"academy": test1_samever}
  _V3={"academy": test1_samever}

  this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}

  run_test_1(this_version_set)
  run_test_entity_status_client(this_version_set)

# like test1_samevers but with additional constraint that version >= dff0...
# because that's where the API was introduced
test2_samevers = ["HERE",
                  "packaging git+https://github.com/academy-agents/academy@main",
                  "packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1"
                  ]

for test2_samever in test2_samevers:

  _V1={"academy": test2_samever}
  _V2={"academy": test2_samever}
  _V3={"academy": test2_samever}

  this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}

  run_test_heartbeat(this_version_set)

# I'm a bit unclear if this should work or not? It does pass for me but
# I should investigate timeout behaviour...
# Constraint-wise, the constraint being tested is that:
# for agent or client >= dff0...  we need the http exchange to be >= dff0

_V1={"academy": "HERE"} # must be at least dff0... for heartbeat protocol
_V2={"academy": "HERE"} # must be at least dff0... for agent client heartbeat settings
_V3={"academy": "packaging academy-py==0.5.0"}
this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}
run_test_1(this_version_set)
run_test_heartbeat(this_version_set)
run_test_entity_status_client(this_version_set)


# this tests an agent which doesn't do heartbeats against a client and exchange
# from the heartbeat era.
# This combo fails, but alok has said he'd like it to work.
_V1={"academy": "HERE"} # must be at least dff0... for heartbeat protocol
_V2={"academy": "packaging academy-py==0.5.0"}
_V3={"academy": "HERE"}
this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}
# run_test_entity_status_client(this_version_set)
