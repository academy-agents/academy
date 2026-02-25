# Frequently Asked Questions

*[Open a new issue](https://github.com/academy-agents/academy/issues){target=_bank} if you have a question not answered in the docs.*

## Logging

### How to enable agent logging in the Manager?

The [`Manager`][academy.manager.Manager] can configure logging when an agent starts on a worker within an executor.

Pass a [`LogConfig`][academy.logging.config.LogConfig] to `Manager.launch`.

```python
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from academy.logging import log_context
from academy.manager import Manager

mp_context = multiprocessing.get_context('spawn')
executor = ProcessPoolExecutor(
    max_workers=3,
    mp_context=mp_context,
)

async with await Manager(..., executors=executor) as manager:
    manager.launch(agent, log_config=...)
```
