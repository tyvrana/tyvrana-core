# Document continuity and strong baselines

Adapter instance IDs select current RPC endpoints. Connection IDs identify a
particular transport lifetime. Neither proves durable content continuity.
Adapters may advertise exactly one read-only, artifact-free operation tagged
`document_attestation`, returning the shared `DocumentAttestation` contract.

`project.attest` establishes document verification metadata:

- `capture`: observe the explicitly bound current document before acceptance or a
  checkpoint. It cannot restore stale accepted state by assuming a current adapter
  is equivalent. Changed accepted content must be reopened and validated normally.
- `reattach`: choose a replacement loaded document/process by matching the durable
  document UUID, complete content digest and format. It does not inherit an old live
  session. A still-connected valid attachment to another host prevents takeover.
- `bootstrap`: for a binding predating strong evidence, require a supplied trusted
  historical artifact SHA256/provenance, a second independent host loaded from that
  artifact, matching document UUIDs, matching complete material digests and a final
  unchanged live observation. A provided checksum is only trustworthy if the caller
  has established its provenance from accepted immutable evidence.

Strong baselines and their verification contexts are durable Core binding metadata,
separate from authored records/checkpoints. Capture/reattach/bootstrap do not rewrite
milestone status, create validation judgments or increment semantic revision.
Existing resource observation context fields carry the stable verification context
once attested; the baseline never depends on a new WebSocket connection ID.

For attestation-capable documents, `project.apply` requires matching strong evidence
before accepting milestones or creating a checkpoint. Bind, verify resources and
capture content first. Unsupported/incomplete content must be resolved, not accepted
through a weaker fingerprint fallback.

Live reads observe current adapter evidence and resolve exactly one matching
host-session/document-session/content combination. Reload/reconnect automatically
uses the new endpoint without revision churn. Different loaded documents or hosts
are unavailable until explicit strong reattachment. Changed content makes the
computed validation/milestone view stale or invalidated while preserving stored
historical acceptance. Reopening unchanged bytes in a new process uses `reattach`;
it does not require manually re-accepting milestones.

All comparisons require complete evidence in the same content format. Resource
structural fingerprints remain useful identity observations; they are not substitutes
for whole-document attestation. Baseline provenance does not make Core a visual critic.
