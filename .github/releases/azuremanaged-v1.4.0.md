# azuremanaged-v1.4.0

## What's Changed

- Update durabletask dependency to v1.4.0
  - Includes restart support, batch actions, work item filtering, timer improvements,
    distributed tracing improvements, updated protobuf definitions, and entity bug fixes
- Add AsyncDurableTaskSchedulerClient for async/await usage with grpc.aio by @andystaples in #115
- Add payload store support for large payload Azure Blob externalization by @andystaples in #124
- Improve timer handling in worker to align with durabletask updates by @andystaples in #122

## External Links

PyPi: [https://pypi.org/project/durabletask.azuremanaged/1.4.0/](https://pypi.org/project/durabletask.azuremanaged/1.4.0/)

Full Changelog: [azuremanaged-v1.3.0...azuremanaged-v1.4.0](https://github.com/microsoft/durabletask-python/compare/azuremanaged-v1.3.0...azuremanaged-v1.4.0)

### Contributors

- @andystaples
