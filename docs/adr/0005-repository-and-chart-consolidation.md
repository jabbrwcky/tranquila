# 0005. Move to pflege-de-labs and bring the Helm chart with it

* Status: Accepted
* Date: 2026-09-14

## Context

tranquila lived under a personal account, `jabbrwcky/tranquila`, while its Helm chart lived in a
third place, `pflege-de/helm-charts/tranquila`. That split had two costs. The chart and the
application it deploys were versioned, reviewed and released in separate repositories, so a
change to a flag and the chart change that exposes it could not be reviewed together or land
atomically. And the application's home did not reflect who maintains it.

The chart's split from the application had already produced drift. Two documented values,
top-level `existingSecret` and `existingConfig`, were read by the templates as
`config.existingSecret` and `config.existing` — the latter a key that appears nowhere in
`values.yaml`. Both features were therefore unreachable through their documented spelling, and
`existingSecret` did not merely no-op: the render failed on the `required` calls guarding the
credential keys. Nothing caught it because the chart had no CI.

## Decision

We will host the application at `pflege-de-labs/tranquila` and keep the chart in it, at
`charts/tranquila`.

The move is a **GitHub repository transfer**, not a fresh repository. A transfer preserves
issues, pull requests, releases and stars, and — the reason it was chosen — installs permanent
redirects from the old path, so existing clones, links and `go get` against the old module path
continue to resolve. The Go module path is nonetheless renamed to
`github.com/pflege-de-labs/tranquila` rather than left relying on the redirect, because a module
path is an identity and not an address.

The chart is published by `.github/workflows/release_helm_chart.yml`, modelled on the equivalent
workflow in `pflege-de-labs/teamster`, to `ghcr.io/<owner>/charts` via chart-releaser. It stays
versioned independently of the application: a push to `main` that raises `version` in
`Chart.yaml` publishes it, anything else is a no-op. Tying the chart's version to the
application's would force a chart release for every application release and vice versa, which is
what the independent `appVersion` field exists to avoid.

A `chart` job in CI lints and renders every values combination under `charts/tranquila/ci/`, and
asserts what the chart produces rather than only that it produced something. Rendering proves a
chart is not broken; it says nothing about a chart that is quietly wrong, which is exactly the
failure the `existingSecret` bug was.

Both `existingSecret` and `existingConfig` now resolve from the documented top-level key **and**
the previously-working nested one, with the documented spelling winning. Reading only the
top-level key would have been the cleaner fix and would have broken every values file that had
worked around the bug by using `config.existingSecret` — the spelling the chart's own notes
recommended.

## Consequences

* The chart and the application change together, in one review, and a flag and the value that
  exposes it can land in the same commit.
* **GHCR packages do not move with a repository transfer.** `ghcr.io/jabbrwcky/tranquila` still
  exists and still serves the images it already had, but nothing new is published there; CI now
  publishes to `ghcr.io/pflege-de-labs/tranquila`, which is what the chart's `image.repository`
  default points at. A deployment pinned to the old path keeps working and stops receiving
  updates — it will not fail loudly, so it has to be found and repointed deliberately.
* Anything importing `github.com/jabbrwcky/tranquila` as a Go module must update its import
  path. tranquila is an application rather than a library, so the expected blast radius is zero,
  but the redirect does not cover this: a module whose `go.mod` declares a different path than
  the one used to fetch it is an error, not a warning.
* The copy in `pflege-de/helm-charts` is removed in a separate pull request there. Until that
  merges, two copies of the chart exist and can drift — the same condition that produced the
  `existingSecret` bug.
* `.gitignore`'s first entry was `tranquila`, intended for the built binary at the repository
  root. Unanchored, it also matched `charts/tranquila/`, so the entire chart would have been
  silently excluded from the commit that added it. It is now anchored as `/tranquila`.
