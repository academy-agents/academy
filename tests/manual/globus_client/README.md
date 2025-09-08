# Globus Client End to End Test

This script validates that the cloud hosted exchange can properly use the Globus API to authenticate users, and that the GlobusExchangeClient can launch agents and delegate tokens to them.

**Running**
First register a Globus client id for the academy exchange server. Run the script:
```bash
python register_exchange_server.py
```

This will print the environment variables that need to be set:
```bash
ACADEMY_TEST_PROJECT_ID=<project_id>
ACADEMY_EXCHANGE_CLIENT_ID=<client_id>
ACADEMY_EXCHANGE_SECRET=<secret>
ACADEMY_GLOBUS_SCOPE_ID=<scope_id>
```

Launch the exchange:
```bash
export ACADEMY_EXCHANGE_CLIENT_ID=<client_id>
export ACADEMY_EXCHANGE_SECRET=<secret>
python -m academy.exchange.cloud --config exchange_config.toml
```

In a different shell
```bash
export ACADEMY_TEST_PROJECT_ID=<project_id>
export ACADEMY_EXCHANGE_CLIENT_ID=<client_id>
export ACADEMY_GLOBUS_SCOPE_ID=<scope_id>
python run_globus_client_test.py
```
