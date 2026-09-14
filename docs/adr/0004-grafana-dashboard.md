# 0004. Ship a Grafana dashboard built from the metrics that exist

* Status: Accepted
* Date: 2026-09-14

## Context

Operating tranquila meant reading logs. The binary has exported OpenTelemetry metrics since the
telemetry package was added, but nothing consumed them, so the questions an operator actually
asks during an incident — is anything moving, is anything failing, is the endpoint the problem,
which bucket is stuck — were answered by grepping `zerolog` output.

Two properties of the existing instrumentation constrain what a dashboard can honestly show, and
both were measured rather than assumed, by running the Prometheus exporter and scraping it:

* **`tranquila.s3.operation.duration` uses the OpenTelemetry default bucket boundaries**, whose
  largest finite bucket is 10 000 ms. The listing pathology this project has spent its recent
  history on lives entirely above that line: the per-attempt deadline starts at 60 s and
  escalates to 240 s. Any `histogram_quantile` above 10 s is extrapolation between the last
  finite bucket and `+Inf`.
* **That histogram carries no `endpoint` attribute** (only `operation`, `bucket`, `status`), so
  source and destination call latency cannot be separated. `tranquila.s3.errors`,
  `tranquila.s3.rate_limit` and `tranquila.s3.rate_limit.changes` do carry it.

A third gap is an absence rather than a shape: there are no discovery metrics at all. Nothing
counts prefixes walked, pages listed, checkpoints saved or resumed, or a prefix parked on a page
the backend will never answer.

## Decision

We will ship `deploy/grafana/tranquila-dashboard.json`, built against the metrics as they are
today, and we will not add instrumentation in the same change.

**Panels are chosen so that no panel is an estimate where an exact answer exists.** Latency is
shown as `rate(sum)/rate(count)` — an exact mean — and the slow tail as
`rate(count) - rate(bucket{le=10000})`, the exact count of calls past the histogram's ceiling.
No p95 or p99 appears anywhere. A p95 of ListObjectsV2 would be the most natural panel to reach
for and the most misleading one available, so its absence is deliberate and is written into the
panel descriptions.

**The dashboard documents its own blind spots in a panel.** The four limits above ship as a text
panel on the dashboard itself, not only in this ADR, because the person misreading a panel at
03:00 is not reading the repository.

**Colour follows the entity, never its rank.** Every closed label set — error class, endpoint,
direction, and the eight S3 operations — gets explicit per-series colour overrides, so filtering
a series out never repaints the survivors. Bucket names are an open set that cannot be enumerated
at authoring time; those panels use Grafana's name-hashed palette, which is stable per bucket for
the same reason, at the cost of not being drawn from the validated palette.

**The palette is theme-invariant, because Grafana's is not.** Fixed colours in dashboard JSON do
not swap between the light and dark themes, so the usual approach of stepping a palette per
surface is unavailable: one set of hexes has to clear every check against *both* Grafana
surfaces (`#ffffff` and `#181b1f`). The eight hues were validated against both and pass every
gate on both — lightness band, chroma floor, adjacent CVD ΔE, the normal-vision floor, and 3:1
contrast — with no warnings in either mode. A severity-ordered assignment for error class
(blue / yellow / red, which would have read more intuitively) was rejected: yellow against red
measures ΔE 13.0 for normal vision, below the 15 floor. Error classes use categorical slots 1–3
instead, with the legend carrying the meaning.

**Two axes are logarithmic**, on the S3-operation and per-bucket duration panels. Those series
span three orders of magnitude — a 19 s listing beside a 12 ms `HeadObject` — and on a linear
axis every fast operation collapses onto the baseline. This is a log scale on *one* axis, not a
second y-axis; the dashboard has no dual-axis panel.

## Consequences

* An operator can answer the common questions without logs. Logs remain the only source for
  discovery progress, which is the one thing this dashboard cannot show.
* The dashboard is worth revisiting if the instrumentation changes. Three changes would each
  unlock a panel that cannot be written today: explicit histogram boundaries for
  `tranquila.s3.operation.duration` extending past 240 s; an `endpoint` attribute on it; and any
  discovery counter at all. None are made here — this change adds no Go code, so it cannot
  regress the sync path.
* `le` is matched with a regex (`le=~"10000(\\.0)?"`) rather than an exact string. Prometheus 3
  normalises bucket bounds to `10000.0` while Prometheus 2 keeps `10000`; an exact match returns
  no data on one of them, and a panel that silently reads empty would be indistinguishable from
  "no slow calls". This was caught by running the query, not by reading it.
* The dashboard ships in three forms from **one** model file: UI import, a Grafana file
  provider, and a `GrafanaDashboard` custom resource for grafana-operator v5. The custom resource
  references the model through a generated ConfigMap rather than inlining it, because an inlined
  copy is a second source of truth that drifts the first time someone edits one and not the other.
  The kustomization root is `deploy/grafana/` rather than a nested directory so the JSON sits at
  or below the root — kustomize refuses to read files above its root without
  `--load-restrictor LoadRestrictionsNone`, and needing a flag to apply a manifest is a worse
  trade than one flat directory. All three are documented in
  [deploy/grafana/README.md](../../deploy/grafana/README.md).
* The operator path needs no datasource configuration, which was verified rather than assumed:
  loading the model with no datasource selected and no URL parameters resolves the `DS_PROMETHEUS`
  template variable to the default Prometheus datasource, and every panel returns data.
  `spec.variables` is documented for pinning it where that default is not wanted.
