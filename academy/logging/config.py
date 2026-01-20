from __future__ import annotations

import abc
import uuid
from collections.abc import Callable


class ObservabilityConfig(abc.ABC):
    """Implementations of this class can initialize observability.

    That means they know how to initialize and de-initialize
    structured logging in the process they are running in,
    in whatever observability context makes sense for the
    object.

    Objects should expect to be pickled/unpickled multiple
    times as they move around an academy distributed system.
    That leads to knock-on requirements like implementing
    classes being importable in all relevant locations, and
    being careful about what state is stored when initializing
    logging.
    """

    def __init__(self):
        self.uuid = str(uuid.uuid4())

    @abc.abstractmethod
    def init_logging(self) -> Callable[[], None]:
        """Initialize logging in current process.

        Hosting environments will call this on the object
        they have been configured with to initialize
        observability.

        It is up for discussion where the relevant points to
        call this are: for example, academy might want to
        initialize logging around multiple agents which all
        exist within a broader htex worker (e.g. when running
        inside Globus Compute).

        In future dev, this should return a callback that will
        uninitialize logging, which can be called by the same hosting
        environments. But for now there's no mechanism to turn off
        logging in a process once started.
        """
        ...
