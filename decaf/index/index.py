import sqlite3

import pandas as pd

from decaf.index import Literal, Structure
from decaf.index.views import construct_views

#
# helper functions
#

def requires_connection(func):
	# wrap function that uses the DB connection
	def database_function(self, *args, **kwargs):
		# check if function is called within an active database connection
		if self.db_connection is None:
			raise RuntimeError(f"The {func.__name__} function must be called within an active database connection context.")
		return func(self, *args, **kwargs)

	return database_function


#
# Main DECAF Index
#

class DecafIndex:
	def __init__(self, db_path):
		self.db_path = db_path
		self.db_connection = None

	def __enter__(self):
		self.connect()
		return self

	def __exit__(self, exception_type, exception_value, exception_traceback):
		self.disconnect()

	def connect(self):
		self.db_connection =  sqlite3.connect(self.db_path)

	def disconnect(self):
		if self.db_connection is not None:
			self.commit()
			self.db_connection.close()
			self.db_connection = None

	@requires_connection
	def commit(self):
		self.db_connection.commit()

	#
	# import functions
	#

	@requires_connection
	def add(self, literals:list[Literal], structures:list[Structure], hierarchies:list[tuple[Structure,Structure]]):
		# insert literals into index (this updates the associated literals' index IDs)
		self.add_literals(literals=literals)
		# insert structures into index (associate structures and previously initialized literal IDs)
		self.add_structures(structures=structures)
		# insert hierarchies into index (based on previously initialized structure IDs)
		self.add_hierarchies(hierarchies=hierarchies)

	@requires_connection
	def add_literals(self, literals:list[Literal]) -> list[Literal]:
		cursor = self.db_connection.cursor()

		for literal in literals:
			# skip literals which already have an entry in the index (i.e., ID that is not None)
			if literal.id is not None:
				continue
			# insert literal into table and get insertion ID
			query = 'INSERT INTO literals (id, start, end, value) VALUES (?, ?, ?, ?)'
			cursor.execute(query, literal.serialize())
			literal.id = int(cursor.lastrowid)

		return literals

	@requires_connection
	def add_structures(self, structures:list[Structure]) -> list[Structure]:
		cursor = self.db_connection.cursor()

		structure_literals = []
		for structure in structures:
			assert all((literal.id is not None) for literal in structure.literals), f"[Error] Please make sure to add all literals to the index before adding the corresponding structures."

			# skip structures which already have an entry in the index (i.e., ID that is not None)
			if structure.id is not None:
				continue

			# insert the structure itself and get the insertion ID
			query = 'INSERT INTO structures (id, start, end, type, value) VALUES (?, ?, ?, ?, ?)'
			cursor.execute(query, structure.serialize())
			structure.id = int(cursor.lastrowid)

			# gather associated literals
			structure_literals += [(structure.id, literal.id) for literal in structure.literals]

		# map constituting literals to structures
		query = 'INSERT INTO structure_literals (structure, literal) VALUES (?, ?)'
		cursor.executemany(query, structure_literals)

		return structures

	@requires_connection
	def add_hierarchies(self, hierarchies:list[tuple[Structure,Structure]]):
		cursor = self.db_connection.cursor()

		assert all((parent.id is not None) and (child.id is not None) for parent, child in hierarchies), f"[Error] Please make sure to add all structures to the index before adding the corresponding hierarchies."

		query = 'INSERT INTO hierarchical_structures (parent, child) VALUES (?, ?)'
		cursor.executemany(query, [(parent.id, child.id) for parent, child in hierarchies])

	#
	# export functions
	#

	@requires_connection
	def export_ranges(self, ranges):
		cursor = self.db_connection.cursor()

		for start, end in ranges:
			query = 'SELECT GROUP_CONCAT(value, "") as export FROM literals WHERE start >= ? AND end <= ?'
			cursor.execute(query, (start, end))
			yield cursor.fetchone()[0]

	@requires_connection
	def export_masked(self, mask_ranges):
		num_literals, _, _ = self.get_size()
		mask_ranges += [(None, num_literals, num_literals)]

		cursor = self.db_connection.cursor()

		start = 0
		for mask_id, mask_start, mask_end in mask_ranges:
			end = mask_start
			query = 'SELECT GROUP_CONCAT(value, "") as export FROM literals WHERE start >= ? AND end <= ?'
			cursor.execute(query, (start, end))
			output = cursor.fetchone()[0]
			if output is not None:
				yield output

			start = mask_end

	#
	# filtering functions
	#

	def _construct_filter_query(self, constraint, output_level):
		#
		# SQL Query Construction Logic
		#
		views = construct_views(constraint=constraint)

		# case: substructures w/o structural constraint (w/o literals)
		relevant_view = 'filtered_substructures'
		output_columns = 'substructure_id, start, end'

		# case: substructures w/o structural constraint (w/ literals)
		if constraint.has_literals():
			relevant_view = 'filtered_literals'

		# case: sequential substructures w/o structural constraint (w/ or w/o literals)
		if constraint.sequential:
			relevant_view = 'filtered_sequences'

		# case: constraint should be applied within a specific structural level
		if constraint.hierarchy is not None:

			# case: output should be at the level of the matching substructures
			if output_level == 'substructures':
				relevant_view = 'filtered_constrained_substructures'

			# case: output should be at the level of the constraining parent structures
			elif output_level == 'structures':
				relevant_view = 'filtered_structures'
				output_columns = 'structure_id, structure_start, structure_end'

			# case: output level does not match the constraint level
			else:
				raise NotImplementedError(f"Unsupported output level '{output_level}'.")

		query = views + f'SELECT {output_columns} FROM {relevant_view}'

		return query

	@requires_connection
	def get_filter_ranges(self, constraint, output_level):
		cursor = self.db_connection.cursor()

		# execute constructed query
		query = self._construct_filter_query(
			constraint=constraint,
			output_level=output_level
		)
		cursor.execute(query)

		return cursor.fetchall()

	@requires_connection
	def filter(self, constraint, output_level='structures'):
		filter_ranges = self.get_filter_ranges(
			constraint=constraint,
			output_level=output_level
		)
		for structure_id, start, end in filter_ranges:
			yield structure_id, start, end, next(self.export_ranges([(start, end)]))

	@requires_connection
	def mask(self, constraint, mask_level='structures'):
		filter_ranges = self.get_filter_ranges(
			constraint=constraint,
			output_level=mask_level
		)
		for output in self.export_masked(mask_ranges=filter_ranges):
			yield output

	#
	# statistics functions
	#

	@requires_connection
	def get_size(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT COUNT(id) FROM literals')
		num_literals = cursor.fetchone()[0]

		cursor.execute('SELECT COUNT(id) FROM structures')
		num_structures = cursor.fetchone()[0]

		cursor.execute('SELECT COUNT(parent) FROM hierarchical_structures')
		num_hierarchies = cursor.fetchone()[0]

		return num_literals, num_structures, num_hierarchies

	@requires_connection
	def get_literal_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT value, COUNT(value) AS total FROM literals GROUP BY value')
		literal_counts = {v: c for v, c in cursor.fetchall()}

		return literal_counts

	@requires_connection
	def get_structure_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT type, COUNT(type) AS total FROM structures GROUP BY type')
		structure_counts = {t: c for t, c in cursor.fetchall()}

		return structure_counts

	@requires_connection
	def get_cooccurence(self, source_constraint, target_constraint):
		# prepare views for easier retrieval
		source_views = construct_views(constraint=source_constraint, view_prefix='source_')
		target_views = construct_views(constraint=target_constraint, view_prefix='target_')

		# select the relevant view based on constraints
		relevant_view = 'filtered_substructures'  # default: relevant structures without constraint
		join_criterion = 'srv.start = trv.start AND srv.end = trv.end'  # default: structures occurring at matching positions

		if (source_constraint.hierarchy is not None) and (target_constraint.hierarchy is not None):
			relevant_view = 'filtered_constrained_substructures'
			join_criterion = 'srv.structure_id = trv.structure_id'  # match at the level of parent structures (e.g., sentences)

		# construct final query
		sources_column = " || ' | ' || ".join(f"'{t}=' || srv.\"type={t}\"" for t in source_constraint.get_types())
		targets_column = " || ' | ' || ".join(f"'{t}=' || trv.\"type={t}\"" for t in target_constraint.get_types())
		query = f'''
		SELECT
	        {sources_column} as sources,
	        {targets_column} as targets,
	        COUNT(*) as frequency
		FROM
		    source_{relevant_view} AS srv
		    JOIN
		    target_{relevant_view} AS trv
		    ON ({join_criterion})
		GROUP BY 
			{', '.join(f'srv."type={t}"' for t in source_constraint.get_types())}, 
			{', '.join(f'trv."type={t}"' for t in target_constraint.get_types())}
		'''
		query = source_views + ', ' + target_views[5:] + query
		cooccurrence = pd.read_sql_query(query, self.db_connection)

		# pivot co-occurrence rows to become a matrix
		cooccurrence = cooccurrence.pivot(
		    index='sources',
		    columns='targets',
		    values='frequency'
		).fillna(0)
		cooccurrence = cooccurrence.astype(int)

		return cooccurrence
