# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit
[https://cla.opensource.microsoft.com](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide a
CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of
Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of
Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Serialization compatibility

Changes to default payload serialization can alter the replay of existing
orchestrations. The [serialization guide](docs/serialization.md) describes the
policy and the current `JsonDataConverter` support matrix. Apply the same replay
assessment to provider adapters, while using each provider's own documented
serialization contract.

For non-major releases:

- Preserve existing encoded payloads, reconstruction, raw fallback, and failure
  behavior except as described below. Defer consideration of new types and
  previously unsupported cases until a breaking major release instead of adding
  feature-specific opt-in flags.
- Replay-breaking changes are limited to security fixes and repairs to
  previously documented supported behavior. A related type or a raw fallback
  alone does not establish support. A change from failure to success can also
  affect replay.
- For an allowed change that can affect replay, explain the exception and impact
  in the pull request, add focused regression coverage including replay where
  appropriate, and provide a changelog warning with actionable upgrade
  guidance. Prefer a compatibility mitigation when feasible and safe.

Use the pull request checklist to identify changes to payloads, reconstructed
values, or failures and describe any replay impact and applicable exception.
Applications can use their own `to_json()` / `from_json()` hooks or a custom
converter rather than expanding the default converter.
