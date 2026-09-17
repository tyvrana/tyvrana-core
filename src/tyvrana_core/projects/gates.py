"""Evaluate authored milestone contracts without interpreting domain meaning."""

from graphlib import TopologicalSorter

from .models import (
    Binding,
    Evidence,
    GateBlocker,
    Issue,
    Milestone,
    RecordView,
    Validation,
)


class MilestoneGates:
    """One transaction's records, effective freshness and prerequisite results."""

    def __init__(self, views: list[RecordView]) -> None:
        self.views = {view.record.id: view for view in views}
        self.milestones = {
            view.record.id: view.record
            for view in views
            if isinstance(view.record, Milestone)
        }
        self.issues = [v.record for v in views if isinstance(v.record, Issue)]
        self.bindings = [v for v in views if isinstance(v.record, Binding)]
        graph = {key: m.prerequisite_ids for key, m in self.milestones.items()}
        self.order = list(TopologicalSorter(graph).static_order())
        self.acceptance: dict[str, list[GateBlocker]] = {}
        self.activation: dict[str, list[GateBlocker]] = {}
        for key in self.order:
            milestone = self.milestones[key]
            prerequisites = []
            for parent_id in milestone.prerequisite_ids:
                parent = self.milestones[parent_id]
                if parent.status != "accepted":
                    prerequisites.append(
                        GateBlocker(
                            milestone_id=key,
                            record_id=parent_id,
                            reason=f"prerequisite:{parent.status}",
                        )
                    )
                prerequisites.extend(self.acceptance[parent_id])
            issues = [
                GateBlocker(milestone_id=key, record_id=i.id, reason="blocking_issue")
                for i in self.issues
                if i.status != "resolved"
                and i.severity in {"major", "critical"}
                and (
                    not i.entity_ids
                    or set(i.entity_ids).intersection(milestone.entity_ids)
                )
            ]
            # Own open issues may require corrective work in this stage. They block
            # acceptance, and therefore downstream activation, not that repair.
            self.activation[key] = self._unique(prerequisites)
            checks = []
            if not milestone.validation_ids or not milestone.acceptance.strip():
                checks.append(
                    GateBlocker(
                        milestone_id=key,
                        record_id=key,
                        reason="acceptance:criteria_and_validation_required",
                    )
                )
            for validation_id in milestone.validation_ids:
                checks.extend(self._validation(key, validation_id))
            self.acceptance[key] = self._unique(prerequisites + issues + checks)

    @staticmethod
    def _unique(items: list[GateBlocker]) -> list[GateBlocker]:
        return list(
            {(b.milestone_id, b.record_id, b.reason): b for b in items}.values()
        )

    def _validation(self, milestone_id: str, key: str) -> list[GateBlocker]:
        view = self.views[key]
        validation = view.record
        assert isinstance(validation, Validation)
        blockers = []

        def block(record_id: str, reason: str) -> None:
            blockers.append(
                GateBlocker(
                    milestone_id=milestone_id, record_id=record_id, reason=reason
                )
            )

        if validation.status != "passed":
            block(key, f"validation:{validation.status}")
        if view.freshness != "current":
            block(key, f"validation:{view.freshness}")
        if not validation.summary.strip() or not validation.evidence_ids:
            block(key, "validation:observations_and_evidence_required")
        for evidence_id in validation.evidence_ids:
            evidence_view = self.views[evidence_id]
            evidence = evidence_view.record
            assert isinstance(evidence, Evidence)
            if not evidence.summary.strip():
                block(evidence_id, "evidence:observations_required")
            if evidence_view.evidence_availability == "expired":
                block(evidence_id, "evidence:expired")
            if evidence.binding_id:
                observation = self.views[evidence.binding_id].binding
                if observation is None or observation.state != "verified":
                    block(evidence.binding_id, "evidence:binding_unverified")
        for binding_view in self.bindings:
            binding = binding_view.record
            assert isinstance(binding, Binding)
            if binding.entity_id in validation.entity_ids:
                observation = binding_view.binding
                if observation is None or observation.state != "verified":
                    block(binding.id, "validation:binding_unverified")
        return blockers
