# JSON Schema

Machine-readable JSON Schema files are available for all three
weevr configuration types. These schemas are generated from the
Pydantic models that the engine uses for validation, so they
always reflect the accepted configuration surface.

## Schema Files

| File | Description |
|------|-------------|
| [`thread.json`][thread] | Thread configuration |
| [`weave.json`][weave] | Weave configuration |
| [`loom.json`][loom] | Loom configuration |
| [`weevr.json`][combined] | Combined (all three) |

[thread]: https://github.com/ardent-data/weevr/blob/main/docs/schema/thread.json
[weave]: https://github.com/ardent-data/weevr/blob/main/docs/schema/weave.json
[loom]: https://github.com/ardent-data/weevr/blob/main/docs/schema/loom.json
[combined]: https://github.com/ardent-data/weevr/blob/main/docs/schema/weevr.json

## Usage with LLMs

Paste the contents of `weevr.json` into your LLM context to
enable accurate YAML generation. For a curated reference
document with examples, see
[llms-full.txt](https://github.com/ardent-data/weevr/blob/main/llms-full.txt).

## Usage with IDE Extensions

Many YAML editor extensions support JSON Schema for
autocompletion and validation. Point the extension at the
raw GitHub URL for the relevant schema file:

```
https://raw.githubusercontent.com/ardent-data/weevr/main/docs/schema/thread.json
```

## Regenerating

Schemas are committed as static files and verified by CI.
To regenerate after model changes:

```bash
python scripts/generate_schema.py
```

To check if schemas are up-to-date without writing:

```bash
python scripts/generate_schema.py --check
```
