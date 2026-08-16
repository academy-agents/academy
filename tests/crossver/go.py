import random

import os
import pathlib
import signal
import subprocess
import time

import z3

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


envs = {}

def create_env(descr: dict) -> pathlib.Path:
    if str(descr) in envs: # this is a bit denormalised so will result in false negatives but not false positives?
        print(f"Using cached environment: {str(descr)}")
        return envs[str(descr)]
 
    env_path = pathlib.Path(".") / ("crossver-env-" + str(random.randint(0,999999999)) + "_" + descr['name'])

    print(f"creating env for {descr} at {env_path}")

    dir = env_path.mkdir()

    here_path = os.getcwd()


    install_target = descr['academy']
    if install_target == "HERE":
        install_target = here_path

    os.system(f"cd {env_path}; virtualenv ./venv; pwd; ls; . ./venv/bin/activate; which python; pip install {install_target}")


    envs[str(descr)] = env_path
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
    v['name'] = "shared"

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

  p1 = subprocess.Popen(f"set -e; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  # a bit of startup time... probs could be done by probing?
  import time
  time.sleep(1)

  base = os.getcwd()

  # V2 should be an agent (against the major-version-consistent API, with no new minor version features)
  # run in the V2 environment.
  p2 = subprocess.Popen(f"set -e; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_1/agent.py", shell=True, process_group=0)

  time.sleep(3)

  # need some time ^ for the agent to get started and write out its agent handle file

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -e; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_1/client.py", shell=True, process_group=0)

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
    v['name'] = "shared"

  v1_env = create_env(version_set['exchange'])
  v2_env = create_env(version_set['agent'])
  v3_env = create_env(version_set['client'])

  exchange_config = """
host = "localhost"
port = 1234
"""

  with open(v1_env / "exchange_config.json", "w") as f:
    f.write(exchange_config)

  p1 = subprocess.Popen(f"set -e; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  import time
  time.sleep(1)

  base = os.getcwd()

  p2 = subprocess.Popen(f"set -e; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_heartbeat/agent.py", shell=True, process_group=0)

  time.sleep(3)

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -e; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_heartbeat/client.py", shell=True, process_group=0)

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
    v['name'] = "shared"

  v1_env = create_env(version_set['exchange'])
  v2_env = create_env(version_set['agent'])
  v3_env = create_env(version_set['client'])

  exchange_config = """
host = "localhost"
port = 1234
"""

  with open(v1_env / "exchange_config.json", "w") as f:
    f.write(exchange_config)

  p1 = subprocess.Popen(f"set -e; cd {v1_env}; . venv/bin/activate; python3 -m academy.exchange.cloud.__main__ --config exchange_config.json", shell=True, process_group=0)

  import time
  time.sleep(1)

  base = os.getcwd()

  p2 = subprocess.Popen(f"set -e; cd {v2_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_entity_status_client/agent.py", shell=True, process_group=0)

  time.sleep(3)

  print("copying agent handle")
  os.system(f"cp {v2_env}/agent.handle {v3_env}/agent.handle")

  p3 = subprocess.Popen(f"set -e; cd {v3_env}; . venv/bin/activate ; python3 {base}/tests/crossver/test_entity_status_client/client.py", shell=True, process_group=0)

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

  # run_test_1(this_version_set)
  # run_test_entity_status_client(this_version_set)

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

  # run_test_heartbeat(this_version_set)

# I'm a bit unclear if this should work or not? It does pass for me but
# I should investigate timeout behaviour...
# Constraint-wise, the constraint being tested is that:
# for agent or client >= dff0...  we need the http exchange to be >= dff0

_V1={"academy": "HERE"} # must be at least dff0... for heartbeat protocol
_V2={"academy": "HERE"} # must be at least dff0... for agent client heartbeat settings
_V3={"academy": "packaging academy-py==0.5.0"}
this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}
#run_test_1(this_version_set)
#run_test_heartbeat(this_version_set)
#run_test_entity_status_client(this_version_set)


# this tests an agent which doesn't do heartbeats against a client and exchange
# from the heartbeat era.
# This combo fails, but alok has said he'd like it to work.
_V1={"academy": "HERE"} # must be at least dff0... for heartbeat protocol
_V2={"academy": "packaging academy-py==0.5.0"}
_V3={"academy": "HERE"}
this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}
# run_test_entity_status_client(this_version_set)


solver = z3.Solver()

# TODO: add v030
AcademyVersion, (v040, v050, v_dff0, v_here) = z3.EnumSort("AcademyVersion",
  ["packaging academy-py==0.4.0",
   "packaging academy-py==0.5.0",
   "packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1",
   "HERE",
  ] 
  )

v1 = z3.Const('v1', AcademyVersion)
v2 = z3.Const('v2', AcademyVersion)
v3 = z3.Const('v3', AcademyVersion)

# here-relevancy constraint, which will be one kind of mode I want to use this in -- for developing new code rather than checking history - that latter mode might be when adding a new test or changing constraint descriptions.
# solver.add(z3.Or(v1 == v_here, v2 == v_here, v3 == v_here))


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

def has_heartbeats(v):
  return z3.Or(v == v_dff0, v == v_here)

solver.add(z3.Implies(has_heartbeats(v2), has_heartbeats(v1)))
solver.add(z3.Implies(has_heartbeats(v3), has_heartbeats(v1)))


# This combination fails because the wire protocol for http exchange
# changed from 0.4.0 to 0.5.0
# [v2 = packaging academy-py==0.5.0, v1 = packaging academy-py==0.4.0, v3 = packaging academy-py==0.4.0]

def speaks_post_050_protocol(v):
  # speaks the post-050 protocol (with or without heartbeats)
  return z3.Or(v == v050, v == v_dff0, v == v_here)

solver.add(z3.Implies(speaks_post_050_protocol(v2), speaks_post_050_protocol(v1)))
solver.add(z3.Implies(speaks_post_050_protocol(v1), speaks_post_050_protocol(v2)))

solver.add(z3.Implies(speaks_post_050_protocol(v3), speaks_post_050_protocol(v1)))
solver.add(z3.Implies(speaks_post_050_protocol(v1), speaks_post_050_protocol(v3)))
# this will interact with the has_heartbeats protocol constraint above too
# which is not bi-directional...

solver.push()

count = 0
while solver.check() == z3.sat:
  count += 1
  m = solver.model()
  print(f"=== test_1: solution {count} ===")
  print(m)
  # when a v is not bound, force a choice. it doesn't matter what.
  chosen_v1 = m[v1] if m[v1] is not None else v040
  chosen_v2 = m[v2] if m[v2] is not None else v040
  chosen_v3 = m[v3] if m[v3] is not None else v040
  solver.add(z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)))#

  _V1={"academy": str(chosen_v1)}
  _V2={"academy": str(chosen_v2)}
  _V3={"academy": str(chosen_v3)}

  this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}

  run_test_1(this_version_set)

# pops the iteration-forcing constraints and anything that is specific
# to particular test case (nothing in this case, but different later).
solver.pop()

# TODO - test these with different constraint sets.

solver.push()

# all the above constraints, plus a constraint that
# the agent definitely has heartbeat support.

solver.add(has_heartbeats(v2))
count = 0
while solver.check() == z3.sat:
  count += 1
  m = solver.model()
  print(f"=== test_heartbeat: solution {count} ===")
  print(m)
  # when a v is not bound, force a choice. it doesn't matter what.
  chosen_v1 = m[v1] if m[v1] is not None else v040
  chosen_v2 = m[v2] if m[v2] is not None else v040
  chosen_v3 = m[v3] if m[v3] is not None else v040
  solver.add(z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)))#

  _V1={"academy": str(chosen_v1)}
  _V2={"academy": str(chosen_v2)}
  _V3={"academy": str(chosen_v3)}

  this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}

  run_test_heartbeat(this_version_set)
solver.pop()

solver.push()
# same as the base compatibility rules
# although I'll probably need to add in an exclusion for an undesired incompatibility

# BUG:
# This combination reports that the agent is inactive.
# v1 = git+https://github.com/academy-agents/academy@main]
# v2 = packaging academy-py==0.5.0,
# v3 = packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1,
# Here's a workaround, but it's not entirely desirable: it means, for example,
# 0.5.0 can't talk to 0.6.0 for agent status.
solver.add(z3.Implies(has_heartbeats(v3), has_heartbeats(v2)))

count = 0
while solver.check() == z3.sat:
  count += 1
  m = solver.model()
  print(f"=== test_entity_status_client: solution {count} ===")
  print(m)
  # when a v is not bound, force a choice. it doesn't matter what.
  chosen_v1 = m[v1] if m[v1] is not None else v040
  chosen_v2 = m[v2] if m[v2] is not None else v040
  chosen_v3 = m[v3] if m[v3] is not None else v040
  solver.add(z3.Not(z3.And(v1 == chosen_v1, v2 == chosen_v2, v3 == chosen_v3)))#

  _V1={"academy": str(chosen_v1)}
  _V2={"academy": str(chosen_v2)}
  _V3={"academy": str(chosen_v3)}

  this_version_set = {"exchange": _V1, "agent": _V2, "client": _V3}

  run_test_entity_status_client(this_version_set)
solver.pop()

