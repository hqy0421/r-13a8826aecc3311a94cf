"""Tests for project-scoped component identity and issue classification."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tap_jira.components import (
    ComponentRegistry,
    build_component_key,
    classify_component_references,
    normalize_identifier,
)
from tap_jira.streams import ComponentStream, IssueStream
from tap_jira.tap import TapJira

if TYPE_CHECKING:
    from collections.abc import Iterable

    import pytest


def _make_tap(state: dict[str, Any] | None = None) -> TapJira:
    return TapJira(
        config={
            "domain": "test.atlassian.net",
            "email": "test@example.com",
            "api_token": "test-token",
        },
        state=state,
    )


def test_build_component_key_is_project_scoped() -> None:
    assert build_component_key("10000", "20000") == "10000:20000"
    assert build_component_key(10000, 20000) == "10000:20000"
    assert build_component_key("10001", "20000") == "10001:20000"
    assert build_component_key(None, "20000") is None
    assert build_component_key("10000", None) is None
    assert build_component_key("  ", "20000") is None


def test_normalize_identifier_handles_jira_id_types() -> None:
    assert normalize_identifier(10000) == "10000"
    assert normalize_identifier(" 10000 ") == "10000"
    assert normalize_identifier("") is None
    assert normalize_identifier(None) is None
    assert normalize_identifier(True) is None  # noqa: FBT003


def test_same_component_name_in_different_projects_is_not_merged() -> None:
    registry = ComponentRegistry()
    registry.remember({"id": "1", "name": "Backend", "projectId": 10000})
    registry.remember({"id": "2", "name": "Backend", "projectId": 10001})

    assert set(registry.entries) == {"10000:1", "10001:2"}
    backend_10000 = registry.get("10000:1")
    backend_10001 = registry.get("10001:2")
    assert backend_10000 is not None
    assert backend_10001 is not None
    assert backend_10000["name"] == "Backend"
    assert backend_10001["name"] == "Backend"

    # An issue in project 10000 resolves only to that project's component.
    result = classify_component_references(
        [{"id": "1", "name": "Backend"}],
        registry,
        project_id="10000",
        project_key="AAA",
    )
    assert [c["status"] for c in result.classifications] == ["resolved"]
    assert result.classifications[0]["componentKey"] == "10000:1"
    assert result.classifications[0]["projectId"] == "10000"
    assert result.unresolved == []


def test_reference_to_component_in_other_project_is_unresolved() -> None:
    registry = ComponentRegistry()
    registry.remember({"id": "1", "name": "Backend", "projectId": 10000})

    result = classify_component_references(
        [{"id": "1", "name": "Backend", "self": "https://example/1"}],
        registry,
        project_id="10001",
        project_key="BBB",
    )

    assert result.classifications[0]["status"] == "unresolved"
    assert result.classifications[0]["reason"] == "unknown_component"
    assert result.unresolved == [
        {
            "componentKey": "10001:1",
            "componentId": "1",
            "projectId": "10001",
            "projectKey": "BBB",
            "name": "Backend",
            "self": "https://example/1",
            "reason": "unknown_component",
        },
    ]


def test_missing_issue_project_id_does_not_guess_across_projects() -> None:
    registry = ComponentRegistry()
    registry.remember({"id": "1", "name": "Backend", "projectId": 10000})
    registry.remember({"id": "1", "name": "Backend", "projectId": 10001})

    result = classify_component_references([{"id": "1", "name": "Backend"}], registry)

    assert result.classifications[0]["status"] == "unresolved"
    assert result.classifications[0]["reason"] == "ambiguous_component_id"
    assert len(result.unresolved) == 1


def test_missing_issue_project_id_resolves_unique_component_id() -> None:
    registry = ComponentRegistry()
    registry.remember(
        {"id": "1", "name": "Backend", "projectId": 10000, "project": "AAA"},
    )

    result = classify_component_references([{"id": "1", "name": "Backend"}], registry)

    assert result.classifications[0]["status"] == "resolved"
    assert result.classifications[0]["componentKey"] == "10000:1"
    assert result.classifications[0]["projectKey"] == "AAA"


def test_registry_survives_component_rename_between_batches() -> None:
    registry = ComponentRegistry()
    registry.remember({"id": "1", "name": "Old name", "projectId": 10000})

    # A later batch renames the same component (same stable ID).
    registry.remember({"id": "1", "name": "New name", "projectId": 10000})

    entry = registry.get("10000:1")
    assert entry is not None
    assert entry["name"] == "New name"
    assert entry["previousNames"] == ["Old name"]

    # An issue still referencing the old category remains explainable.
    result = classify_component_references(
        [{"id": "1", "name": "Old name"}],
        registry,
        project_id="10000",
    )
    assert result.classifications[0]["status"] == "resolved"
    assert result.classifications[0]["name"] == "Old name"
    assert result.classifications[0]["catalogName"] == "New name"
    assert result.unresolved == []


def test_registry_is_carried_across_batches_via_tap_state() -> None:
    state: dict[str, Any] = {
        "bookmarks": {
            "components": {
                "componentRegistry": {
                    "10000:1": {
                        "componentKey": "10000:1",
                        "componentId": "1",
                        "projectId": "10000",
                        "projectKey": "AAA",
                        "name": "Backend",
                        "previousNames": [],
                    },
                },
            },
        },
    }
    tap = _make_tap(state)

    result = classify_component_references(
        [{"id": "1", "name": "Backend"}],
        ComponentRegistry.from_tap_state(tap.state),
        project_id="10000",
        project_key="AAA",
    )
    assert result.classifications[0]["status"] == "resolved"
    assert result.unresolved == []


def test_component_stream_record_preserves_project_id_and_key() -> None:
    tap = _make_tap()
    stream = ComponentStream(tap)

    record = stream.post_process(
        {"id": "1", "name": "Backend", "projectId": 10000, "project": "AAA"},
        context={"projectId": "10000"},
    )

    assert record["projectId"] == "10000"
    assert record["componentKey"] == "10000:1"
    assert record["project"] == "AAA"

    registry = ComponentRegistry.from_tap_state(tap.state)
    entry = registry.get("10000:1")
    assert entry is not None
    assert entry["projectId"] == "10000"


def test_component_stream_uses_context_project_when_missing() -> None:
    tap = _make_tap()
    stream = ComponentStream(tap)

    record = stream.post_process(
        {"id": "2", "name": "Backend"},
        context={"projectId": "10001"},
    )

    assert record["projectId"] == "10001"
    assert record["componentKey"] == "10001:2"


def test_issue_post_process_emits_classifications_and_unresolved() -> None:
    tap = _make_tap()
    registry = ComponentRegistry.from_tap_state(tap.state, create=True)
    registry.remember({"id": "1", "name": "Backend", "projectId": 10000})

    stream = IssueStream(tap)
    record = stream.post_process(
        {
            "id": "ISSUE-1",
            "fields": {
                "created": "2026-01-01T00:00:00.000+0000",
                "updated": "2026-01-02T00:00:00.000+0000",
                "project": {"id": "10000", "key": "AAA"},
                "components": [
                    {"id": "1", "name": "Backend"},
                    {"id": "9", "name": "Merged away"},
                ],
            },
        },
        context=None,
    )

    assert record["projectId"] == "10000"
    assert record["projectKey"] == "AAA"

    classifications = record["componentClassifications"]
    assert [c["status"] for c in classifications] == ["resolved", "unresolved"]
    assert classifications[0]["componentKey"] == "10000:1"
    assert classifications[1]["componentKey"] == "10000:9"
    assert classifications[1]["reason"] == "unknown_component"

    unresolved = record["unresolvedComponentReferences"]
    assert len(unresolved) == 1
    assert unresolved[0]["componentId"] == "9"
    assert unresolved[0]["projectId"] == "10000"
    assert unresolved[0]["name"] == "Merged away"


def test_issue_post_process_without_components() -> None:
    stream = IssueStream(_make_tap())
    record = stream.post_process(
        {
            "id": "ISSUE-2",
            "fields": {
                "created": "2026-01-01T00:00:00.000+0000",
                "updated": "2026-01-02T00:00:00.000+0000",
                "project": {"id": "10000", "key": "AAA"},
            },
        },
        context=None,
    )

    assert record["componentClassifications"] == []
    assert record["unresolvedComponentReferences"] == []


def test_component_stream_collects_per_project(monkeypatch: pytest.MonkeyPatch) -> None:
    from tap_jira import streams as streams_module  # noqa: PLC0415

    tap = _make_tap()

    monkeypatch.setattr(
        streams_module.ProjectStream,
        "get_records",
        lambda _self, _context: iter(
            [{"id": "10000"}, {"id": "10001"}, {"name": "no id"}],
        ),
    )

    def fake_project_components(
        _self: object,
        context: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        project_id = context["projectId"]
        yield {"id": f"{project_id}-1", "name": "Backend", "projectId": project_id}

    monkeypatch.setattr(
        streams_module._ProjectComponentStream,  # noqa: SLF001
        "get_records",
        fake_project_components,
    )

    stream = ComponentStream(tap)
    records = list(stream.get_records(None))

    # The project without an ID is skipped rather than guessed.
    assert [record["projectId"] for record in records] == ["10000", "10001"]
    assert [record["id"] for record in records] == ["10000-1", "10001-1"]


def test_component_stream_registers_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tap_jira import streams as streams_module  # noqa: PLC0415

    tap = _make_tap()

    monkeypatch.setattr(
        streams_module.ProjectStream,
        "get_records",
        lambda _self, _context: iter([{"id": "10000"}]),
    )
    monkeypatch.setattr(
        streams_module._ProjectComponentStream,  # noqa: SLF001
        "get_records",
        lambda _self, context: iter(
            [{"id": "1", "name": "Backend", "projectId": context["projectId"]}],
        ),
    )

    stream = ComponentStream(tap)
    first = [stream.post_process(r, None) for r in stream.get_records(None)]
    second = [stream.post_process(r, None) for r in stream.get_records(None)]

    assert first[0]["componentKey"] == "10000:1"
    assert second[0]["componentKey"] == "10000:1"

    registry = ComponentRegistry.from_tap_state(tap.state)
    assert list(registry.entries) == ["10000:1"]
    entry = registry.get("10000:1")
    assert entry is not None
    assert entry["name"] == "Backend"


def test_component_sync_persists_registry_for_issue_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Components synced in one run remain usable for issue classification."""
    from tap_jira import streams as streams_module  # noqa: PLC0415

    tap = _make_tap()

    monkeypatch.setattr(
        streams_module.ProjectStream,
        "get_records",
        lambda _self, _context: iter([{"id": "10000", "key": "AAA"}]),
    )
    monkeypatch.setattr(
        streams_module._ProjectComponentStream,  # noqa: SLF001
        "get_records",
        lambda _self, context: iter(
            [{"id": "7", "name": "Billing", "projectId": context["projectId"]}],
        ),
    )

    components = ComponentStream(tap)
    list(components._sync_records(write_messages=False))  # noqa: SLF001

    assert tap.state["bookmarks"]["components"]["componentRegistry"] == {
        "10000:7": {
            "componentKey": "10000:7",
            "componentId": "7",
            "projectId": "10000",
            "projectKey": None,
            "name": "Billing",
            "previousNames": [],
        },
    }

    issues = IssueStream(tap)
    record = issues.post_process(
        {
            "id": "ISSUE-3",
            "fields": {
                "created": "2026-01-01T00:00:00.000+0000",
                "updated": "2026-01-02T00:00:00.000+0000",
                "project": {"id": "10000", "key": "AAA"},
                "components": [{"id": "7", "name": "Billing"}],
            },
        },
        context=None,
    )

    assert record["unresolvedComponentReferences"] == []
    assert record["componentClassifications"][0]["componentKey"] == "10000:7"
    assert record["componentClassifications"][0]["status"] == "resolved"


def test_component_merged_away_between_batches_stays_explainable() -> None:
    state: dict[str, Any] = {}

    # First batch: two components in the same project.
    registry = ComponentRegistry(state)
    registry.remember({"id": "1", "name": "Billing", "projectId": 10000})
    registry.remember({"id": "2", "name": "Invoices", "projectId": 10000})

    # Second batch: component 2 was merged into 1 and no longer exists.
    still_registered = ComponentRegistry(state)
    still_registered.remember({"id": "1", "name": "Billing", "projectId": 10000})

    # An issue still referencing the merged-away component is explained by the
    # registration from the first batch rather than reported as anonymous.
    result = classify_component_references(
        [{"id": "2", "name": "Invoices"}],
        still_registered,
        project_id="10000",
        project_key="AAA",
    )

    assert result.classifications[0]["status"] == "resolved"
    assert result.classifications[0]["componentKey"] == "10000:2"
    assert result.classifications[0]["catalogName"] == "Invoices"
    assert result.unresolved == []


def test_reference_with_own_project_id_is_scoped_by_that_project() -> None:
    registry = ComponentRegistry()
    registry.remember({"id": "5", "name": "Backend", "projectId": 10001})

    result = classify_component_references(
        [{"id": "5", "name": "Backend", "projectId": "10001"}],
        registry,
        project_id="10000",
        project_key="AAA",
    )

    # The issue's project (10000) wins over the reference's own projectId, so a
    # component ID that belongs to another project is not silently attributed.
    assert result.classifications[0]["status"] == "unresolved"
    assert result.classifications[0]["reason"] == "unknown_component"
    assert result.classifications[0]["componentKey"] == "10000:5"
