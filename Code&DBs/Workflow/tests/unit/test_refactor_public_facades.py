from __future__ import annotations


def test_runtime_workflow_unified_public_facade_imports() -> None:
    from runtime.workflow.unified import (
        get_run_status,
        preview_workflow_execution,
        run_worker_loop,
        submit_workflow,
        wait_for_run,
    )

    assert callable(submit_workflow)
    assert callable(get_run_status)
    assert callable(preview_workflow_execution)
    assert callable(run_worker_loop)
    assert callable(wait_for_run)


def test_provider_onboarding_public_facade_imports() -> None:
    from registry.provider_onboarding import (
        load_provider_onboarding_spec_from_file,
        normalize_provider_onboarding_spec,
        run_provider_onboarding,
    )

    assert callable(load_provider_onboarding_spec_from_file)
    assert callable(normalize_provider_onboarding_spec)
    assert callable(run_provider_onboarding)


def test_frontdoor_public_facade_imports() -> None:
    from surfaces.api.frontdoor import NativeFrontdoorError, NativeWorkflowFrontdoor, health, status, submit

    assert NativeFrontdoorError.__name__ == "NativeFrontdoorError"
    assert NativeWorkflowFrontdoor.__name__ == "NativeWorkflowFrontdoor"
    assert callable(submit)
    assert callable(status)
    assert callable(health)
