<img src="decaf.png" alt="DECAF Logo" style="height: 10em"/>

# DECAF: A Dynamically Extensible Corpus Analysis Framework

DECAF is an open-source Python framework that enables fine-grained linguistic analyses and filtering of existing datasets for generating targeted training interventions for LM generalization research.

## Getting Started

For basic analyses and filtering, DECAF can be installed without any external dependencies:

```bash
pip install decaffinate
```

For importing datasets, and for more advanced analyses, please install the package with external dependencies:

```bash
pip install decaffinate[full]
```

For getting a quick overview of DECAF's core functionalities, we recommend taking a look at [the demo notebook](experiments/acl-demo/acl-demo.ipynb).

## Building an Index

Rather than creating new resources for each experiment, DECAF builds **indices** over datasets with existing linguistic annotations, and leverages them to analyze, filter, and generate highly controlled and reproducible experimental settings targeting specific research questions.  It maintains extensibility by constructing separate indices over raw text (literals) and annotations (structures).

Indexing is specific to each dataset format, so please refer to [the import documentation](scripts/import/README.md) for details. In general, the import scripts follow the simple structure:

```bash
script/import/format.py \
	--input /path/to/data.txt \
	--output /path/to/index
```

After having built the index, you can query it using the `DecafIndex` class:

```python
from decaf import DecafIndex

di = DecafIndex('/path/to/index')
literals = di.get_literal_counts()
structures = di.get_structure_counts()
```

## Building a Filter

DECAF treats indices and filters as independent entities from the original corpus. This means, that indices can be continually extended with new annotation layers, and that filters can be transferred across datasets.

Filters are constructed using the `Filter` class, which contains `Constraint` objects, which in turn contain a `Condition` sequence.

```python
Filter([
  Criterion([
    Condition(
      stype='type1',
      values=['label1'],
      literals=['form1']
    ),
    Condition(
      stype='type2',
      values=['label2.1', 'label2.2'],
      literals=['form2']
    )],
    operation='AND'
	)],
	sequential=True,
	hierarchy=['sentence', 'token']
)
```

A `Condition` specified what to match at the structure level, i.e., the structure type, its value (if any), and specific surface forms (if any). Within a `Criterion`, multiple conditions, or nested criteria, can be combined using boolean operations. Finally, the top-level criteria or wrapped in a `Filter`, which can enforce whether the criteria can occur anywhere, or in a direct sequence. If the index contains hierarchical information, we can further enforce that the criteria must apply within a certain hierarchical level, e.g., token annotations occurring within a self-contained sentence.

## Analyzing an Index

Once an index is built, we can analyze its statistics using general, as well as filter-specific queries, e.g.:

```python
di = DecafIndex('/path/to/index')

# get the database sizes
num_literals, num_structures, num_hierarchies = di.get_size()

# get the frequency of each literal
literals = di.get_literal_counts()

# get the frequency of each structure
structures = di.get_structure_counts()
total_of_type = di.get_structure_counts(types=['type'])
type_value_counts = di.get_structure_counts(types=['token'], values=True)
type_value_form_counts = di.get_structure_counts(types=['token'], values=True, literals=True)

# get the co-occurence across two filters
cooccurrence = di.get_cooccurrence(
  source_filter=df,
  target_filter=df
)
```

## Exporting Data

DECAF supports exporting filtered versions of the original data, either by keeping only the matched structures, or alternatively, by masking them out.

```python
# exporting filter results
outputs = di.filter(constraint=df)
# masking filter results
outputs = di.mask(constraint=df)
```

By default, this will return/mask any structure that is matched. However, sometimes, we want to be more precise and remove structures, that are matched within a hierarchical constraint (e.g., relative clauses within their main clause). In these cases, we specify an `output_level`, which differs from the matched structure itself:

```python
# exporting filter results
outputs = di.filter(
  constraint=df,
  output_level='substructure'
)
# masking filter results
outputs = di.mask(
  constraint=df,
  output_level='substructure'
)
```

## Sharing

DECAF indices can be easily shared, as they are self-contained within their respective directories. Simply zip them up, and publish.

Similarly, filters are transferable across datasets, since they query the underlying, unified index, instead of the original corpus itself.

We provide some example [`experiments/`](experiments/), and highly encourage everyone to share their DECAF experiments, as well! ☕️