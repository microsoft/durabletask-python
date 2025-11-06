# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.4.0

- Updates base dependency to durabletask v0.5.0
  - Adds support for Durable Entities

## v0.3.1

- Updates base dependency to durabletask v0.4.1
  - Fixed an issue where orchestrations would still throw non-determinism errors even when versioning logic should have prevented it

## v0.3.0

- Updates base dependency to durabletask v0.4.0
  - Added support for orchestration and activity tags
  - Added support for orchestration versioning and versioning logic in the worker
