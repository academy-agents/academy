from __future__ import annotations

academy_debug_mode = False


def set_academy_debug(debug_mode: bool = True) -> None:
    """Set academy debug mode.

    When debug mode is set, errors in critical tasks will
    raise a SystemExit exception causing the thread to exit.
    These critical tasks are expected not to raise exceptions,
    and any exception indicate something within Academy has
    broken.
    """
    global academy_debug_mode  # noqa: PLW0603
    academy_debug_mode = debug_mode


def get_academy_debug() -> bool:
    """Fetch academy debug mode."""
    return academy_debug_mode
