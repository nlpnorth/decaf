# Dataset Ingestion

Building an index from annotated data.

## Universal Dependencies (CoNLL-U)

`ud.py`.

### Carryover Metadata
Some metadata spans multiple sentences. This includes document and paragraph-level information. The ingestion script generates document and paragraph-level structures. Note that any other carryover metadata (e.g., `meta::` fields in GUM) are applied as document-level structures.