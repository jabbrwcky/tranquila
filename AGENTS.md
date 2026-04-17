# AGENTS.md

General operating instructions for coding agents working in this repository.

## 1) Core Goals

- Deliver correct, minimal, maintainable changes.
- Preserve existing behavior unless a change is explicitly requested.
- Prefer small, reviewable diffs over broad refactors.
- Leave the codebase in a better state than you found it.

## 2) Working Style

- Start by understanding the request and relevant code paths.
- Share a brief plan for multi-step tasks before editing.
- Execute end-to-end when possible: implement, validate, and summarize.
- Ask clarifying questions only when blocked by ambiguity or missing requirements.

## 3) Editing Rules

- Change only what is necessary for the task.
- Keep naming, structure, and style consistent with nearby code.
- Do not make unrelated formatting changes.
- Avoid introducing new dependencies unless clearly justified.
- Never commit secrets, tokens, or credentials.

## 4) Safety and Git Hygiene

- Never run destructive git commands (for example force reset/checkout) unless explicitly requested.
- Do not revert user changes that are unrelated to the task.
- If unexpected modifications appear during work, pause and call them out.
- Prefer atomic changes that are easy to inspect and rollback.

## 5) Validation Requirements

- Run the smallest useful validation first, then expand if needed.
- At minimum, verify:
  - Build/compile succeeds for changed code.
  - Relevant tests pass (or explain why they were not run).
  - Lint/format checks pass when configured.
- If validation fails, fix issues introduced by your change before finishing.

## 6) Language and Tooling Conventions

- Follow repository conventions from existing files and config.
- For Go code in this repo:
  - Keep code `gofmt`-clean.
  - Prefer table-driven tests where practical.
  - Handle errors explicitly and avoid panics in normal control flow.

## 7) Communication to Humans

- Be concise and factual.
- Report what changed, why, and how it was validated.
- Include file paths and key implementation notes.
- Clearly list any assumptions, risks, or follow-up items.

## 8) Definition of Done

A task is complete when:

1. Requested behavior is implemented.
2. Relevant checks/tests were run (or a clear reason is provided).
3. No unrelated files were changed.
4. Final summary is provided with next steps only if useful.
