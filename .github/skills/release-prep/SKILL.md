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

### 4. Validate

- Run diagnostics on changed markdown and TOML files.
- Fix formatting or heading issues introduced by release prep changes.
- Verify the final diff only contains release-prep updates.

### 5. Wait for merge and tags before release drafting

Before creating draft releases in GitHub UI, require explicit user
confirmation of both conditions:

- The version-bump/release-prep PR is merged
- Tags `v<version>` and `azuremanaged-v<version>` already exist in the target
  repository

If either condition is not met, stop after preparing release body text and ask
the user to confirm once merge and tags are complete.

### 6. Draft GitHub release bodies

Draft two release body texts for the GitHub Releases UI (do not add files to
the repository):

- Tag: `v<version>`
- Tag: `azuremanaged-v<version>`

Match existing release structure:

- Title (`# v<version>` or `# azuremanaged-v<version>`)
- `## What's Changed`
- `## External Links`
- `### Contributors`

Include:

- PyPI link for the exact release version
- Full changelog compare link
- Contributor handles from the commit range
- Keep drafts in the assistant response (or PR comment) so they can be pasted
  directly into the Releases section


## Output

Return a short summary with:

- Updated files
- Commit coverage confirmation
- Any manual follow-ups (for example, tag creation or publishing)
