from __future__ import annotations

import pytest

# Mark all tests in tests/integration as integration tests so they
# can be selected or deselected with:
#   pytest -k "integration"
#   pytest -k "not integration"
pytestmark = pytest.mark.integration
