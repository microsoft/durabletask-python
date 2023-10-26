# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### New

- Retry policies for activities and sub-orchestrations ([#11](https://github.com/microsoft/durabletask-python/pull/11)) - contributed by [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a5

### New

- Adds support for secure channels ([#18](https://github.com/microsoft/durabletask-python/pull/18)) - contributed by [@elena-kolevska](https://github.com/elena-kolevska)

### Fixed

- Fix zero argument values sent to activities as None ([#13](https://github.com/microsoft/durabletask-python/pull/13)) - contributed by [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a3

### New

- Add gRPC metadata option ([#16](https://github.com/microsoft/durabletask-python/pull/16)) - contributed by [@DeepanshuA](https://github.com/DeepanshuA)

### Changes

- Removed Python 3.7 support due to EOL ([#14](https://github.com/microsoft/durabletask-python/pull/14)) - contributed by [@berndverst](https://github.com/berndverst)

## v0.1.0a2

### New

- Continue-as-new ([#9](https://github.com/microsoft/durabletask-python/pull/9))
- Support for Python 3.7+ ([#10](https://github.com/microsoft/durabletask-python/pull/10)) - contributed by [@DeepanshuA](https://github.com/DeepanshuA)

## v0.1.0a1

Initial release, which includes the following features:

- Orchestrations and activities
- Durable timers
- Sub-orchestrations
- Suspend, resume, and terminate client operations
