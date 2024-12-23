# Dataset Ingestion

Building an index from annotated data.

## Universal Dependencies (CoNLL-U)

`ud.py`.

### Trailing Spaces
By default, a trailing whitespace is added as a separate atom after each token which does not specify the `SpaceAfter=No` metadata field (under `MISC`). Token-level annotations do not include this additional space character.

### Carryover Metadata
Some metadata spans multiple sentences. This includes document and paragraph-level information. The ingestion script generates document and paragraph-level structures. Note that any other carryover metadata (e.g., `meta::` fields in GUM) are applied as document-level structures.