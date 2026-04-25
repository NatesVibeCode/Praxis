from __future__ import annotations

import inspect
import re

from contracts.data_contracts import SUPPORTED_DATA_OPERATIONS
from runtime import data_plane


def test_supported_data_operations_match_data_plane_dispatcher() -> None:
    source = inspect.getsource(data_plane.execute_data_job)
    dispatcher_operations = set(
        re.findall(r'job\["operation"\]\s*==\s*"([^"]+)"', source)
    )

    assert dispatcher_operations == set(SUPPORTED_DATA_OPERATIONS)
