---
name: release-prep
description: >-
  Prepare a release for durabletask, durabletask.azuremanaged, or
  azure-functions-durable. Use when the user asks for release prep, version
  bumping, changelog updates, or release body drafting. Trigger phrases include:
  release prep, prepare vX.Y.Z, changelog for release, and draft GitHub release
  notes.
---

# Release Prep

This skill prepares a coordinated release for all packages in this repository:

- `durabletask`
- `durabletask.azuremanaged`
- `azure-functions-durable`

The skill accepts a target version (for example `1.4.0`) and performs the
required changes consistently. A single-package release is an exception: only
release one package when the user explicitly requests it, and record that
exception in the release-preparation instructions for that release.

## Inputs

- `version`: Target semantic version (for example `1.4.0`)
- Optional: `packages` limits the release to named packages only when the user
  explicitly requests a single-package release.
- Optional: `baseTag` overrides for comparison if tags are non-standard

If `version` is not provided, ask the user before continuing.

## Steps

### 1. Determine source range and collect commits

- Root package range: `v<previousVersion>..HEAD`
- Azure managed package range: `azuremanaged-v<previousVersion>..HEAD`
- Azure Functions package range: `azurefunctions-v<previousVersion>..HEAD`
- Use commit subjects and touched files to classify each change as:
  - `durabletask` only
  - `durabletask.azuremanaged` only
  - `azure-functions-durable` only
  - shared/infra/docs changes

### 2. Update package versions

Update all project versions:

- `pyproject.toml` -> `version = "<version>"`
- `durabletask-azuremanaged/pyproject.toml` -> `version = "<version>"`
- `azure-functions-durable/pyproject.toml` -> `version = "<version>"`

Update Azure Managed dependency floors:

- `durabletask>=<version>`
- `durabletask[azure-blob-payloads]>=<version>`

Update the `azure-functions-durable` `durabletask` dependency floor when the
coordinated release establishes a new compatible core SDK minimum. For an
explicit single-package Azure Functions release, do not change that floor
solely because the provider package version changes.

### 3. Update changelogs

- Add a new `## v<version>` section directly under `## Unreleased` in every
  package's changelog. Create `## Unreleased` if it is absent:
  - `CHANGELOG.md` for `durabletask`
  - `durabletask-azuremanaged/CHANGELOG.md` for `durabletask.azuremanaged`
  - `azure-functions-durable/CHANGELOG.md` for `azure-functions-durable`
- Ensure user-facing changes since the previous release tags are represented.
- Keep entries concise and grouped by type (`ADDED`, `CHANGED`, `FIXED`, `REMOVED`) where
  applicable.
- Follow the repository's unindented changelog style. Changelogs are not
  covered by CI Markdown linting, so review their formatting manually.
- Exclude internal-only changes from changelogs (for example CI/workflow-only
  updates, test-only changes, and implementation refactors with no public
  behavior or API impact).

### 4. Validate

- Run diagnostics on changed markdown and TOML files.
- Fix formatting or heading issues introduced by release prep changes.
- Verify the final diff only contains release-prep updates.

### 5. Wait for merge and tags before release drafting

Before creating draft releases in GitHub UI, require explicit user
confirmation of both conditions:

- The version-bump/release-prep PR is merged
- Tags for every package already exist in the target repository:
  `v<version>` for `durabletask`, `azuremanaged-v<version>` for
  `durabletask.azuremanaged`, and `azurefunctions-v<version>` for
  `azure-functions-durable`

If either condition is not met, stop after preparing release body text and ask
the user to confirm once merge and tags are complete.

### 6. Draft GitHub release bodies

Draft three release body texts for the GitHub Releases UI (do not add files to
the repository):

- `durabletask`: `v<version>`
- `durabletask.azuremanaged`: `azuremanaged-v<version>`
- `azure-functions-durable`: `azurefunctions-v<version>`

Match existing release structure:

- Title matching the package tag
- `## What's Changed`
- `## External Links`
- `### Contributors`

Include:

- PyPI link for the exact release version
- Full changelog compare link
- Contributor handles from the commit range
- Keep drafts in the assistant response (or PR comment) so they can be pasted
  directly into the Releases section
- Keep the release body focused on user-facing changes and avoid internal-only
  details (CI/test updates or implementation-only notes)


## Output

Return a short summary with:

- Updated files
- Commit coverage confirmation
- Any manual follow-ups (for example, tag creation or publishing)
