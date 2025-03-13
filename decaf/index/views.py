#
# SQL Sub-queries Mega Library
#


# basic substructures (w/o literals)

def relevant_substructures(constraint):
	# view containing all potentially relevant substructures (w/o literals)
	# e.g., all upos=(NOUN|ADJ)
	view = f'''
	SELECT
        id as substructure_id,
        ROW_NUMBER() OVER (ORDER BY start) AS position,
        start,
        end,
        {', '.join(f'MAX(CASE WHEN type = "{t}" THEN value END) as "type={t}"' for t in constraint.get_types())}
    FROM structures
    WHERE type IN ({', '.join("'" + t + "'" for t in constraint.get_types())})
    GROUP BY start, end
    ORDER BY start, end'''
	return view


def filtered_substructures(constraint, view_prefix=''):
	# view containing all matching substructures (w/o literals)
	view = f'''
    SELECT *
    FROM {view_prefix}relevant_substructures 
    WHERE {constraint.to_sql(literals=False)}'''
	return view


# basic substructures (w/ literals)

def relevant_literals(constraint, view_prefix=''):
	# view containing all potentially relevant substructures (w/ literals for types with literal constraints)
	view = f'''
	SELECT
        substructures.substructure_id AS substructure_id,
        substructures.position AS position, substructures.start AS start, substructures.end AS end,
        {', '.join(f'substructures."type={t}" AS "type={t}"' for t in constraint.get_types())},
        GROUP_CONCAT(literals.value, '') AS literal
    FROM
        {view_prefix}filtered_substructures AS substructures
    JOIN
        structure_literals
    ON (substructures.substructure_id = structure_literals.structure)
    JOIN
        literals
    ON (structure_literals.literal = literals.id)
    GROUP BY structure_literals.structure
	'''
	return view


def filtered_literals(constraint, view_prefix=''):
	# containing all matching substructures (w/ literals)
	view = f'''
	SELECT * 
	FROM {view_prefix}relevant_literals 
	WHERE {constraint.to_sql(literals=True)}'''
	return view


# sequential substructures (w/ and w/o literals)

def filtered_sequences_pivot(constraint, view_prefix=''):
	assert len(constraint.criteria) > 0, f"[Error] Sequence filtering requires constraints with at least 1 condition. {len(constraint.criteria)} condition(s) provided."
	sequential_views = []
	for cidx, criterion in enumerate(constraint.criteria):
		# construct view matching condition at current sequential substructure
		sequential_views.append('(' + f'''
		SELECT
            substructure_id AS seq{cidx}_substructure_id, position AS seq{cidx}_position, start AS seq{cidx}_start, end AS seq{cidx}_end,
            {', '.join(f'"type={t}" AS "seq{cidx}_type={t}"' for t in constraint.get_types())},
            {'literal' if criterion.has_literals() else 'NULL'} AS seq{cidx}_literal
        FROM {f'{view_prefix}filtered_literals' if criterion.has_literals() else f'{view_prefix}filtered_substructures'}
        WHERE {criterion.to_sql()}''' + ')')
		# add positional join condition for subsequent substructures
		if cidx > 0:
			sequential_views[-1] += f' ON (seq{cidx}_position = seq{cidx-1}_position + 1)'

	# construct overall sequential join sequence
	view = f'''
    SELECT *
    FROM ''' + '\n JOIN \n'.join(sequential_views)

	return view


def filtered_sequences(constraint, view_prefix=''):
	assert len(constraint.criteria) > 0, f"[Error] Sequence filtering requires constraints with at least 1 condition. {len(constraint.criteria)} condition(s) provided."
	sequential_views = []
	for cidx in range(len(constraint.criteria)):
		sequential_views.append(f'''
		    SELECT
		        seq{cidx}_substructure_id AS substructure_id, seq{cidx}_position AS position, seq{cidx}_start AS start, seq{cidx}_end AS end,
		        {', '.join(f'"seq{cidx}_type={t}" AS "type={t}"' for t in constraint.get_types())},
		        seq{cidx}_literal AS literal
		    FROM {view_prefix}filtered_sequences_pivot''')

	view = '\n UNION \n'.join(sequential_views) + '\n ORDER BY start'

	return view


# structural constraints

def relevant_structures(constraint, view_prefix):
	# view over potentially relevant parent constraint structures
	assert constraint.hierarchy is not None, f"[Error] Cannot construct view of parent structures if no hierarchy is provided."

	# select the relevant filtered view (default: non-sequential structures w/o literals)
	filtered_view = f'{view_prefix}filtered_substructures'
	if constraint.has_literals():
		filtered_view = f'{view_prefix}filtered_literals'
	if constraint.sequential:
		filtered_view = f'{view_prefix}filtered_sequences'

	# construct joins across specified hierarchy
	joins = []
	subsumed_id = 'substructure_id'
	for level_idx, level in enumerate(reversed(constraint.hierarchy)):
		is_root = (level_idx == len(constraint.hierarchy) - 1)
		level_id = 'structure_id' if is_root else f'level{level_idx}_id'
		joins.append(f'''
			JOIN
		        hierarchical_structures AS hs{level_idx}
		    JOIN (
		        SELECT id AS {level_id}{', start AS structure_start, end AS structure_end' if is_root else ''}
		        FROM structures
		        WHERE type = "{level}"
		    )
		    ON ({level_id} = hs{level_idx}.parent AND {subsumed_id} = hs{level_idx}.child)
			''')
		subsumed_id = str(level_id)

	# construct view joining matched substructures with containing constraint structures
	view = f'''
	SELECT
        structure_id, structure_start, structure_end, substructure_id, position, start, end,
        {', '.join(f'"type={t}"' for t in constraint.get_types())}
        {', literal' if constraint.has_literals() else ''}
    FROM
		{filtered_view}\n''' + '\n'.join(joins)

	return view


def filtered_structures(constraint, view_prefix=''):
	# view over parent structures containing all relevant substructures

	view = f'''
    SELECT  structure_id, structure_start, structure_end
    FROM {view_prefix}relevant_structures
    GROUP BY structure_id
    HAVING ({constraint.to_grouped_sql()})'''

	return view


def filtered_constrained_substructures(constraint, view_prefix=''):
	# view over substructures contained in constraining parent structure which contain all relevant substructures

	view = f'''
    SELECT
        relevant.structure_id AS structure_id, substructure_id, start, end,
        {', '.join(f'"type={t}"' for t in constraint.get_types())}
		{', literal' if constraint.has_literals() else ''}
    FROM
        {view_prefix}relevant_structures AS relevant
    JOIN
        {view_prefix}filtered_structures AS filtered
    ON (filtered.structure_id = relevant.structure_id)'''

	return view


# all conjoined views

def construct_views(constraint, view_prefix=''):
	views = {}

	# substructure-level views (w/o literals)
	views['relevant_substructures'] = relevant_substructures(constraint=constraint)
	views['filtered_substructures'] = filtered_substructures(constraint=constraint, view_prefix=view_prefix)
	# substructure-level views (w/ literals)
	if constraint.has_literals():
		views['relevant_literals'] = relevant_literals(constraint=constraint, view_prefix=view_prefix)
		views['filtered_literals'] = filtered_literals(constraint=constraint, view_prefix=view_prefix)
	# sequential substructure-level views (w/ or w/o literals)
	if constraint.sequential:
		views['filtered_sequences_pivot'] = filtered_sequences_pivot(constraint=constraint, view_prefix=view_prefix)
		views['filtered_sequences'] = filtered_sequences(constraint=constraint, view_prefix=view_prefix)
	# substructures constrained within parent structure-level (w/ or w/o literals)
	if constraint.hierarchy is not None:
		views['relevant_structures'] = relevant_structures(constraint=constraint, view_prefix=view_prefix)
		views['filtered_structures'] = filtered_structures(constraint=constraint, view_prefix=view_prefix)
		views['filtered_constrained_substructures'] = filtered_constrained_substructures(constraint=constraint, view_prefix=view_prefix)

	# construct query prefix with all available views
	views = 'WITH ' + '\n, '.join(f'{view_prefix}{name} AS ({definition})' for name, definition in views.items()) + '\n'

	return views