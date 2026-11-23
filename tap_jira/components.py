"""Component identity and reference classification helpers.

Jira component names are only unique *within* a project. Two projects may each
define a component named e.g. ``Backend`` and they are unrelated. On top of
that, components can be merged/renamed over time, while issues (or historical
data) may still point at a component that no longer exists.

This module therefore gives every component a stable, project-scoped key of the
form ``<projectId>:<componentId>``:

* The project ID is kept explicitly, so records from different projects are
  never merged even when their component names match.
* The component ID is stable across renames, so the key does not change when a
  component is renamed.
* Component names are never used to match or merge records.
* A registry of previously seen components is persisted in the tap state, so
  references observed in a later batch can still be explained using the names
  recorded in an earlier batch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, MutableMapping
    from typing import Any

COMPONENTS_STREAM_NAME = "components"
"""Name of the stream that owns the persisted component registry."""

COMPONENT_KEY_SEPARATOR = ":"
"""Separator between the project ID and the component ID in a component key."""

REGISTRY_STATE_KEY = "componentRegistry"
"""Key under the components stream state that holds the component registry."""

RESOLVED = "resolved"
UNRESOLVED = "unresolved"

REASON_MISSING_COMPONENT_ID = "missing_component_id"
REASON_MISSING_PROJECT_ID = "missing_project_id"
REASON_UNKNOWN_COMPONENT = "unknown_component"
REASON_AMBIGUOUS_COMPONENT_ID = "ambiguous_component_id"


def normalize_identifier(value: object) -> str | None:
    """Return a normalized, non-empty string identifier.

    Jira returns project and component IDs both as strings and as integers,
    depending on the endpoint. Normalizing them keeps component keys stable.

    Args:
        value: Raw identifier value.

    Returns:
        The stripped string form, or ``None`` when no usable identifier exists.
    """
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def build_component_key(
    project_id: object,
    component_id: object,
) -> str | None:
    """Build a stable, project-scoped component key.

    The key intentionally includes the project ID: component names/IDs from
    different projects are never conflated. Names are never part of the key.

    Args:
        project_id: ID of the project the component belongs to.
        component_id: ID of the component within that project.

    Returns:
        ``"<projectId>:<componentId>"``, or ``None`` when either ID is missing.
    """
    project = normalize_identifier(project_id)
    component = normalize_identifier(component_id)
    if project is None or component is None:
        return None
    return f"{project}{COMPONENT_KEY_SEPARATOR}{component}"


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


class ComponentRegistry:
    """Persistent, project-scoped registry of known components.

    The registry is backed by the components stream state, which means it is
    carried across sync batches via the tap state. Entries are keyed only by
    :func:`build_component_key`; they are never keyed or merged by name.

    Entries are never removed. A component that disappears from a later batch
    (e.g. because it was merged into another component) can still be explained
    using its earlier registration.
    """

    def __init__(self, stream_state: MutableMapping[str, Any] | None = None) -> None:
        """Create a registry wrapper.

        Args:
            stream_state: Writable state dictionary of the components stream.
                When omitted, the registry is an in-memory-only registry.
        """
        self._stream_state: MutableMapping[str, Any] = (
            stream_state if stream_state is not None else {}
        )

    @property
    def entries(self) -> dict[str, dict[str, Any]]:
        """Return a read-only snapshot of the registered components."""
        raw = self._stream_state.get(REGISTRY_STATE_KEY)
        if not isinstance(raw, dict):
            return {}
        return {
            key: dict(value)
            for key, value in raw.items()
            if isinstance(key, str) and isinstance(value, dict)
        }

    def _writable_entries(self) -> dict[Any, Any]:
        raw = self._stream_state.get(REGISTRY_STATE_KEY)
        if not isinstance(raw, dict):
            raw = {}
            self._stream_state[REGISTRY_STATE_KEY] = raw
        return raw

    def get(self, component_key: str) -> dict[str, Any] | None:
        """Look up a component by its stable key.

        Args:
            component_key: Project-scoped key from :func:`build_component_key`.

        Returns:
            A copy of the registry entry, or ``None`` when unknown.
        """
        entry = self.entries.get(component_key)
        return dict(entry) if entry is not None else None

    def find_by_component_id(self, component_id: object) -> list[dict[str, Any]]:
        """Find every known component with the given component ID.

        Jira component IDs are usually unique across projects, but this method
        deliberately returns *all* candidates so that callers can flag
        ambiguous references instead of silently picking a project.

        Args:
            component_id: Component ID to search for.

        Returns:
            A list of registry entries (possibly empty).
        """
        wanted = normalize_identifier(component_id)
        if wanted is None:
            return []
        return [
            dict(entry)
            for entry in self.entries.values()
            if normalize_identifier(entry.get("componentId")) == wanted
        ]

    @classmethod
    def from_tap_state(
        cls,
        tap_state: Mapping[str, Any],
        *,
        create: bool = False,
    ) -> ComponentRegistry:
        """Load the registry persisted in the tap state.

        The registry lives in the ``components`` stream bookmark, so it is
        carried across sync batches together with the rest of the state.

        Args:
            tap_state: The tap's state dictionary.
            create: When ``True``, create the components bookmark if missing so
                that registrations are persisted. When ``False`` the state is
                read without modification, which keeps deselected streams from
                adding bookmarks to the state.

        Returns:
            A registry backed by the persisted components stream state.
        """
        writable = cast("MutableMapping[str, Any]", tap_state)
        bookmarks = writable.get("bookmarks")
        stream_state: Any = None
        if isinstance(bookmarks, dict):
            stream_state = bookmarks.get(COMPONENTS_STREAM_NAME)

        if not isinstance(stream_state, dict):
            if not create:
                return cls({})
            bookmarks = writable.setdefault("bookmarks", {})
            stream_state = bookmarks.setdefault(COMPONENTS_STREAM_NAME, {})
        if not isinstance(stream_state, dict):
            return cls({})
        return cls(stream_state)

    def remember(self, component: Mapping[str, Any]) -> str | None:
        """Register (or refresh) a component from a components-stream record.

        Registration is keyed by ``(projectId, id)``. Renames update the
        displayed ``name`` but preserve the historic names in ``previousNames``
        so that categories recorded in an earlier batch remain interpretable.

        Args:
            component: Component record containing at least ``projectId`` and
                ``id``. ``name`` and the project key are recorded when
                available.

        Returns:
            The component's stable key, or ``None`` when it cannot be built.
        """
        project_id = normalize_identifier(component.get("projectId"))
        component_id = normalize_identifier(component.get("id"))
        component_key = build_component_key(project_id, component_id)
        if component_key is None:
            return None

        entries = self._writable_entries()
        existing = entries.get(component_key)
        name = normalize_identifier(component.get("name"))

        if not isinstance(existing, dict):
            entries[component_key] = {
                "componentKey": component_key,
                "componentId": component_id,
                "projectId": project_id,
                "projectKey": normalize_identifier(component.get("project")),
                "name": name,
                "previousNames": [],
            }
            return component_key

        self._merge_entry(existing, component, name)
        return component_key

    @staticmethod
    def _merge_entry(
        entry: dict[str, Any],
        component: Mapping[str, Any],
        name: str | None,
    ) -> None:
        previous_names = entry.get("previousNames")
        if not isinstance(previous_names, list):
            previous_names = []
            entry["previousNames"] = previous_names

        recorded_name = normalize_identifier(entry.get("name"))
        if recorded_name is None:
            if name is not None:
                entry["name"] = name
        elif name is not None and name != recorded_name:
            _append_unique(previous_names, recorded_name)
            entry["name"] = name

        project_key = normalize_identifier(component.get("project"))
        if project_key is not None and entry.get("projectKey") is None:
            entry["projectKey"] = project_key


@dataclass(frozen=True)
class ComponentClassification:
    """Result of classifying an issue's component references."""

    classifications: list[dict[str, Any]]
    """One entry per referenced component, resolved or not."""

    unresolved: list[dict[str, Any]]
    """The subset of references that could not be resolved against the registry."""


def _classification_entry(  # noqa: PLR0913
    reference: Mapping[str, Any],
    entry: Mapping[str, Any] | None,
    *,
    component_key: str | None,
    project_id: str | None,
    project_key: str | None,
    reason: str | None,
) -> dict[str, Any]:
    """Build the per-reference classification returned to the issue record."""
    return {
        "componentKey": component_key,
        "componentId": normalize_identifier(reference.get("id")),
        "projectId": project_id,
        "projectKey": project_key or (entry.get("projectKey") if entry else None),
        "name": normalize_identifier(reference.get("name")),
        "catalogName": entry.get("name") if entry else None,
        "status": RESOLVED if entry else UNRESOLVED,
        "reason": reason,
    }


def _unresolved_entry(
    reference: Mapping[str, Any],
    *,
    component_key: str | None,
    project_id: str | None,
    project_key: str | None,
    reason: str,
) -> dict[str, Any]:
    """Build an unresolved-reference entry, preserving the raw reference."""
    return {
        "componentKey": component_key,
        "componentId": normalize_identifier(reference.get("id")),
        "projectId": project_id,
        "projectKey": project_key,
        "name": normalize_identifier(reference.get("name")),
        "self": reference.get("self"),
        "reason": reason,
    }


def _resolve_reference(
    reference: Mapping[str, Any],
    registry: ComponentRegistry,
    issue_project_id: str | None,
    issue_project_key: str | None,
) -> tuple[
    dict[str, Any] | None,
    str | None,
    str | None,
    str | None,
    str | None,
]:
    """Resolve one component reference against the registry.

    Returns:
        A tuple of ``(entry, component_key, project_id, project_key, reason)``.
    """
    component_id = normalize_identifier(reference.get("id"))
    reference_project_id = normalize_identifier(reference.get("projectId"))
    project_id = issue_project_id or reference_project_id
    project_key = issue_project_key

    if component_id is None:
        return None, None, project_id, project_key, REASON_MISSING_COMPONENT_ID

    if project_id is not None:
        key = build_component_key(project_id, component_id)
        entry = registry.get(key) if key is not None else None
        if entry is not None:
            return entry, key, project_id, project_key, None
        return None, key, project_id, project_key, REASON_UNKNOWN_COMPONENT

    # Without a project ID we cannot build a stable key. Match by component ID
    # only, and refuse to guess when the ID maps to more than one project.
    candidates = registry.find_by_component_id(component_id)
    if len(candidates) == 1:
        entry = candidates[0]
        return (
            entry,
            normalize_identifier(entry.get("componentKey")),
            normalize_identifier(entry.get("projectId")),
            normalize_identifier(entry.get("projectKey")),
            None,
        )
    if len(candidates) > 1:
        return None, None, None, None, REASON_AMBIGUOUS_COMPONENT_ID
    return None, None, None, None, REASON_MISSING_PROJECT_ID


def classify_component_references(
    references: Iterable[Mapping[str, Any]] | None,
    registry: ComponentRegistry,
    *,
    project_id: object = None,
    project_key: str | None = None,
) -> ComponentClassification:
    """Classify an issue's component references against the registry.

    References are matched by component ID only; names are never used. The
    issue's project ID scopes the lookup, so identically named components in
    different projects are never conflated.

    Args:
        references: The raw ``fields.components`` list from an issue.
        registry: Registry of components seen so far.
        project_id: ID of the project the issue belongs to.
        project_key: Key of the project the issue belongs to.

    Returns:
        The full classification of every reference plus the unresolved subset.
    """
    classifications: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    issue_project_id = normalize_identifier(project_id)
    issue_project_key = normalize_identifier(project_key)

    for reference in references or []:
        if not isinstance(reference, dict):
            continue

        entry, key, resolved_project_id, resolved_project_key, reason = (
            _resolve_reference(
                reference,
                registry,
                issue_project_id,
                issue_project_key,
            )
        )

        classifications.append(
            _classification_entry(
                reference,
                entry,
                component_key=key,
                project_id=resolved_project_id,
                project_key=resolved_project_key,
                reason=reason,
            ),
        )

        if entry is None:
            unresolved.append(
                _unresolved_entry(
                    reference,
                    component_key=key,
                    project_id=resolved_project_id,
                    project_key=resolved_project_key,
                    reason=reason or REASON_UNKNOWN_COMPONENT,
                ),
            )

    return ComponentClassification(
        classifications=classifications,
        unresolved=unresolved,
    )
