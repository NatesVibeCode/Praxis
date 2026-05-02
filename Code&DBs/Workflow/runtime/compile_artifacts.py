"""Legacy import path for materialize artifact authority.

`runtime.materialize_artifacts` owns the implementation. Keeping this module
as a re-export prevents stale internal imports from reaching retired
compile-artifact storage.
"""

from runtime.materialize_artifacts import *  # noqa: F403
