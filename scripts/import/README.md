# Data Import

Building an index from annotated data.

## Universal Dependencies (CoNLL-U)

```bash
scripts/import/ud.py --input /path/to/data.conllu --output /path/to/index
```

### Carryover Metadata
Some metadata spans multiple sentences. This includes document and paragraph-level information. The ingestion script generates document and paragraph-level structures. Note that any other carryover metadata (e.g., `meta::` fields in GUM) are applied as document-level structures.

### Intermediate Whitespaces
By default, a trailing whitespace is added as a separate literal after each token which does not specify the `SpaceAfter=No` metadata field (under `MISC`). Alternatively, the `--force-alignment` flag can be set, in order to match tokens back to the original sentence `text`, inserting whitespaces where needed.

### Sentence Terminator
By default, a whitespace is added after each sentence to prevent exported sentences from following each other directly. This character can be changed using the `--sentence-terminator` flag (e.g., to `"\n"` or `""`).