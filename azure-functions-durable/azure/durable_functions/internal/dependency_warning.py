# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Best-effort check for Function App dependencies that allow a new major."""

import os
import re
import shlex
from importlib.metadata import PackageNotFoundError, version
from logging import Logger
from pathlib import Path

from packaging.markers import UndefinedComparison, UndefinedEnvironmentName
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

_PACKAGE = "azure-functions-durable"
_INDEX_OPTIONS_WITH_VALUE = {
    "-i", "-f", "--index-url", "--extra-index-url", "--find-links",
    "--trusted-host", "--only-binary", "--no-binary",
}
_INDEX_FLAGS = {"--no-index", "--pre", "--prefer-binary"}


def _read_specifiers(root: Path, logger: Logger) -> SpecifierSet | None:
    specifiers: list[str] = []
    required = False
    active: set[tuple[Path, bool]] = set()
    visited: set[tuple[Path, bool]] = set()

    def read_file(path: Path, constraint: bool) -> bool:
        nonlocal required
        try:
            path = path.resolve()
            key = (path, constraint)
            if key in active:
                logger.debug("Cannot check Durable Functions dependencies: cyclic requirements in %s.", path)
                return False
            if key in visited:
                return True
            content = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            logger.debug("Cannot read Function App requirements from %s: %s", path, exc)
            return False

        active.add(key)
        try:
            for raw_line in content.splitlines():
                line = re.sub(r"\s+#.*$", "", raw_line).strip()
                if not line or line.startswith("#"):
                    continue
                if line.endswith("\\"):
                    logger.debug("Cannot check multiline requirements in %s.", path)
                    return False
                if line.startswith("-"):
                    try:
                        parts = shlex.split(line)
                    except ValueError:
                        logger.debug("Cannot parse a requirements option in %s.", path)
                        return False
                    option = parts[0]
                    argument = None
                    if option in ("-r", "--requirement", "-c", "--constraint") and len(parts) == 2:
                        argument = parts[1]
                    elif option.startswith(("--requirement=", "--constraint=")) and len(parts) == 1:
                        option, argument = option.split("=", 1)
                    elif option.startswith(("-r", "-c")) and len(option) > 2 and len(parts) == 1:
                        option, argument = option[:2], option[2:]
                    elif (option in _INDEX_FLAGS and len(parts) == 1) or (
                            option in _INDEX_OPTIONS_WITH_VALUE and len(parts) == 2) or (
                            option.split("=", 1)[0] in _INDEX_OPTIONS_WITH_VALUE
                            and "=" in option and len(parts) == 1):
                        continue
                    if not argument or "://" in argument or "${" in argument:
                        logger.debug("Cannot inspect a requirements option in %s.", path)
                        return False
                    nested_constraint = constraint or option in ("-c", "--constraint")
                    if not read_file(path.parent / argument, nested_constraint):
                        return False
                    continue
                try:
                    requirement = Requirement(line)
                except InvalidRequirement:
                    logger.debug("Cannot parse a requirement in %s.", path)
                    return False
                if canonicalize_name(requirement.name) != _PACKAGE:
                    continue
                try:
                    if requirement.marker is not None and not requirement.marker.evaluate():
                        continue
                except (UndefinedComparison, UndefinedEnvironmentName, InvalidVersion) as exc:
                    logger.debug("Cannot evaluate a requirement marker in %s: %s", path, exc)
                    return False
                if requirement.url is not None:
                    logger.debug("Cannot determine the Durable Functions version from a URL in %s.", path)
                    return False
                required = required or not constraint
                specifiers.append(str(requirement.specifier))
        finally:
            active.remove(key)
        visited.add(key)
        return True

    if not read_file(root / "requirements.txt", False) or not required:
        return None
    return SpecifierSet(",".join(specifiers))


def _allows_later_major(specifiers: SpecifierSet, installed: Version) -> bool:
    candidates: set[Version] = set()

    def add_release(epoch: int, major: int, minor: int, patch: int) -> None:
        prefix = f"{epoch}!" if epoch else ""
        candidates.add(Version(f"{prefix}{major}.{minor}.{patch}"))

    for major in (installed.major, installed.major + 1, installed.major + 2):
        add_release(installed.epoch, major, 0, 0)
        add_release(installed.epoch, major, 0, 1)
        add_release(installed.epoch, major, 1, 0)

    for specifier in specifiers:
        try:
            bound = Version(specifier.version.removesuffix(".*"))
        except InvalidVersion:
            continue
        candidates.add(bound)
        add_release(bound.epoch, bound.major, bound.minor, bound.micro + 1)
        add_release(bound.epoch, bound.major, bound.minor + 1, 0)
        add_release(bound.epoch, bound.major + 1, 0, 0)

    for candidate in tuple(candidates):
        base = candidate.base_version
        candidates.update(Version(f"{base}{suffix}") for suffix in (".dev0", "a0", "b0", "rc0"))
        if candidate.pre and candidate.pre[1] > 0:
            label, number = candidate.pre
            candidates.add(Version(f"{base}{label}{number - 1}"))

    allowed = [candidate for candidate in candidates if specifiers.contains(candidate, prereleases=True)]
    return (
        any(candidate.major == installed.major for candidate in allowed)
        and any(candidate.major > installed.major and candidate > installed for candidate in allowed)
    )


def warn_for_function_directory(function_directory: str, logger: Logger) -> None:
    """Find the Function App root before checking its dependencies."""
    roots: list[Path] = []
    configured_root = os.environ.get("AzureWebJobsScriptRoot")
    if configured_root:
        roots.append(Path(configured_root))
    directory = Path(function_directory)
    roots.extend((directory, *directory.parents))
    try:
        roots.append(Path.cwd())
    except OSError as exc:
        logger.debug("Cannot determine the Function App working directory: %s", exc)
    for root in roots:
        if (root / "host.json").is_file():
            warn_if_unbounded(str(root), logger)
            return
    logger.debug("Cannot find the Function App root for %s.", function_directory)


def warn_if_unbounded(function_directory: str, logger: Logger) -> None:
    """Warn when the app's requirements demonstrably permit a later major."""
    root = Path(function_directory)
    try:
        specifiers = _read_specifiers(root, logger)
    except RecursionError as exc:
        logger.debug("Cannot check deeply nested Function App requirements: %s", exc)
        return
    if specifiers is None:
        return
    try:
        installed = Version(version(_PACKAGE))
        allows_later_major = _allows_later_major(specifiers, installed)
    except (PackageNotFoundError, InvalidVersion) as exc:
        logger.debug("Cannot determine the installed Durable Functions version: %s", exc)
        return
    if allows_later_major:
        next_major = f"{installed.epoch}!" if installed.epoch else ""
        next_major += str(installed.major + 1)
        logger.warning(
            "The Function App requirements allow azure-functions-durable to upgrade beyond major version %d. "
            "Future major releases may include broad, replay-breaking changes. If a deployment installs these "
            "requirements without a lock, an automatic upgrade could cause existing orchestrations to fail "
            "unexpectedly. Pin the major version, for example azure-functions-durable>=%s,<%s, "
            "or deploy using a complete dependency lock.",
            installed.major, installed.public, next_major,
        )
