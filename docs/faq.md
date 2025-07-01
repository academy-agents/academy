# Frequently Asked Questions

*[Open a new issue](https://github.com/proxystore/academy/issues){target=_bank} if you have a question not answered in the FAQ, Guides, or API docs.*

## Logging

### How to enable agent logging in the Manager?

The [`Manager`][academy.manager.Manager] does not configure logging when an agent starts on a worker within an executor.
We recommend using the worker initialization features of executors to configure logging, such as by calling [`init_logging()`][academy.logging.init_logging] or [`logging.basicConfig()`][logging.basicConfig].
For example, use the `initializer` argument when using a [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor].

```python
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from academy.logging import init_logging
from academy.manager import Manager

mp_context = multiprocessing.get_context('spawn')
executor = ProcessPoolExecutor(
    max_workers=3,
    initializer=init_logging,
    initargs=(logging.INFO,),
    mp_context=mp_context,
)

async with await Manager(..., executors=executor) as manager:
    ...
```

## Best Practices

This section highlights common best practices for developing applications in Academy.

### Avoid communication operations during behavior initialization

The `__init__` method of a [`Behavior`][academy.behavior.Behavior] is called in one of two places:

1. On the client when submitting an instantiated behavior to be executed.
1. On the agent at the start of the setup sequence when the behavior instantiation is deferred.

In both scenarios, it is unsafe to perform communication operations (i.e., invoking an action on a remote agent) in `__init__` because connection resources and background tasks have not yet been initialized.

The [`Behavior.on_setup()`][academy.behavior.Behavior.on_setup] callback can be used instead to perform communication once the agent is in a running state.
