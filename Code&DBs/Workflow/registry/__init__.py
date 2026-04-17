"""Registry authority package."""

from . import agent_config
from .domain import (
    RuntimeProfileAuthorityRecord,
    ContextBundle,
    RegistryBoundaryError,
    RegistryResolutionError,
    RegistryResolver,
    RuntimeProfile,
    WorkspaceAuthorityRecord,
    WorkspaceIdentity,
)
from .repository import (
    PostgresRegistryAuthorityRepository,
    RegistryRepositoryError,
    bootstrap_registry_authority_schema,
    load_registry_resolver,
)
from .context_bundle_repository import (
    ContextBundleAnchorRecord,
    ContextBundleRepositoryError,
    ContextBundleSnapshot,
    PostgresContextBundleRepository,
    bootstrap_context_bundle_schema,
)
from .endpoint_failover import (
    PostgresProviderFailoverAndEndpointAuthorityRepository,
    ProviderEndpointAuthoritySelector,
    ProviderEndpointBindingAuthorityRecord,
    ProviderFailoverAndEndpointAuthority,
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    ProviderFailoverAuthoritySelector,
    ProviderFailoverBindingAuthorityRecord,
    load_provider_failover_and_endpoint_authority,
)

__all__ = [
    "RuntimeProfileAuthorityRecord",
    "ContextBundle",
    "agent_config",
    "ContextBundleAnchorRecord",
    "ContextBundleRepositoryError",
    "ContextBundleSnapshot",
    "PostgresContextBundleRepository",
    "PostgresProviderFailoverAndEndpointAuthorityRepository",
    "PostgresRegistryAuthorityRepository",
    "ProviderEndpointAuthoritySelector",
    "RegistryBoundaryError",
    "RegistryRepositoryError",
    "RegistryResolutionError",
    "RegistryResolver",
    "ProviderEndpointBindingAuthorityRecord",
    "ProviderFailoverAndEndpointAuthority",
    "ProviderFailoverAndEndpointAuthorityRepositoryError",
    "ProviderFailoverAuthoritySelector",
    "ProviderFailoverBindingAuthorityRecord",
    "RuntimeProfile",
    "WorkspaceAuthorityRecord",
    "WorkspaceIdentity",
    "bootstrap_context_bundle_schema",
    "bootstrap_registry_authority_schema",
    "load_provider_failover_and_endpoint_authority",
    "load_registry_resolver",
]
