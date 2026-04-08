---
name: release-prep
description: >-
  Prepare a release for durabletask and durabletask.azuremanaged. Use when the
  user asks for release prep, version bumping, changelog updates, or release
  body drafting. Trigger phrases include: release prep, prepare vX.Y.Z,
  changelog for release, and draft GitHub release notes.
---

# Release Prep

This skill prepares a coordinated release for both packages in this repository:

- `durabletask`
- `durabletask.azuremanaged`

The skill accepts a target version (for example `1.4.0`) and performs the
required changes consistently.

## Inputs

- `version`: Target semantic version (for example `1.4.0`)
- Optional: `baseTag` overrides for comparison if tags are non-standard

If `version` is not provided, ask the user before continuing.

## Steps

### 1. Determine source range and collect commits

- Root package range: `v<previousVersion>..HEAD`
- Azure managed package range: `azuremanaged-v<previousVersion>..HEAD`
- Use commit subjects and touched files to classify each change as:
  - `durabletask` only
  - `durabletask.azuremanaged` only
  - shared/infra/docs changes

### 2. Update package versions

Update both project versions:

- `pyproject.toml` -> `version = "<version>"`
- `durabletask-azuremanaged/pyproject.toml` -> `version = "<version>"`

Update azuremanaged dependency floors:

- `durabletask>=<version>`
- `durabletask[azure-blob-payloads]>=<version>`

### 3. Update changelogs

- Add a new `## v<version>` section directly under `## Unreleased` in:
  - `CHANGELOG.md`
  - `durabletask-azuremanaged/CHANGELOG.md`
- Ensure all commits since the previous release tags are represented.
- Keep entries concise and grouped by type (`ADDED`, `CHANGED`, `FIXED`, `REMOVED`) where
  applicable.

### 4. Draft GitHub release bodies

Create draft release body markdown files in `.github/releases/`:

- `.github/releases/v<version>.md`
- `.github/releases/azuremanaged-v<version>.md`

Match existing release structure:

- Title (`# v<version>` or `# azuremanaged-v<version>`)
- `## What's Changed`
- `## External Links`
- `### Contributors`

Include:

- PyPI link for the exact release version
- Full changelog compare link
- Contributor handles from the commit range

### 5. Validate

- Run diagnostics on changed markdown and TOML files.
- Fix formatting or heading issues introduced by release prep changes.
- Verify the final diff only contains release-prep updates.

## Output

Return a short summary with:

- Updated files
- Commit coverage confirmation
- Any manual follow-ups (for example, tag creation or publishing)
