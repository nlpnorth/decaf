import sqlite3

import pandas as pd
from bleach.callbacks import target_blank

from decaf.index import Atom, Structure
from decaf.index.views import construct_views

#
# helper functions
#

def requires_database(func):
	# wrap function that uses the DB connection
	def wrapped_func(self, *args, **kwargs):
		# check if function is called within an active database connection
		if self.db_connection is None:
			raise RuntimeError(f"The {func.__name__} function must be called within an active database connection context.")
		return func(self, *args, **kwargs)

	return wrapped_func


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
			self.db_connection.close()
			self.db_connection = None

	#
	# import functions
	#

	@requires_database
	def add_atoms(self, atoms:list[Atom]):
		cursor = self.db_connection.cursor()

		query = 'INSERT INTO atoms (id, start, end, value) VALUES (?, ?, ?, ?)'
		cursor.executemany(query, [atom.serialize() for atom in atoms])

		self.db_connection.commit()

	@requires_database
	def add_structures(self, structures:list[Structure]):
		cursor = self.db_connection.cursor()

		query = 'INSERT INTO structures (id, start, end, value, type, subsumes) VALUES (?, ?, ?, ?, ?, ?)'
		cursor.executemany(query, [structure.serialize() for structure in structures])

		self.db_connection.commit()

	#
	# export functions
	#

	@requires_database
	def export_ranges(self, ranges):
		cursor = self.db_connection.cursor()

		for start, end in ranges:
			query = 'SELECT GROUP_CONCAT(value, "") as export FROM atoms WHERE start >= ? AND end <= ?'
			cursor.execute(query, (start, end))
			yield cursor.fetchone()[0]

	#
	# filtering functions
	#

	def _construct_filter_query(self, constraint, output_level):
		#
		# SQL Query Construction Logic
		#
		views = construct_views(constraint=constraint, output_level=output_level)

		# case: substructures w/o structural constraint (w/o literals)
		relevant_view = 'filtered_substructures'
		output_columns = 'substructure_id, start, end'

		# case: substructures w/o structural constraint (w/ literals)
		if constraint.has_literals():
			relevant_view = 'filtered_literals'

		# case: sequential substructures w/o structural constraint (w/ or w/o literals)
		if constraint.sequential:
			relevant_view = 'filtered_sequences'

		# case: output level should be at a specific structural level
		if output_level is not None:
			relevant_view = 'relevant_structures'
			output_columns = 'DISTINCT structure_id, structure_start, structure_end'

		# case: constraint should be applied within a specific structural level
		if constraint.level is not None:

			# case: output should be at the level of the matching substructures
			if output_level is None:
				relevant_view = 'filtered_constrained_substructures'

			# case: output should be at the level of the constraining parent structures
			elif (output_level is not None) and (output_level == constraint.level):
				relevant_view = 'filtered_structures'
				output_columns = 'structure_id, structure_start, structure_end'

			# case: output level does not match the constraint level
			else:
				raise NotImplementedError(
					f"For structurally constrained queries, output levels besides the constraint or match level are unsupported. Specified output level: '{output_level}'.")

		query = views + f'SELECT {output_columns} FROM {relevant_view}'

		return query

	@requires_database
	def get_filter_ranges(self, constraint, output_level):
		cursor = self.db_connection.cursor()

		# execute constructed query
		query = self._construct_filter_query(
			constraint=constraint,
			output_level=output_level
		)
		cursor.execute(query)

		return cursor.fetchall()

	@requires_database
	def filter(self, constraint, output_level = None):
		filter_ranges = self.get_filter_ranges(
			constraint=constraint,
			output_level=output_level
		)
		for structure_id, start, end in filter_ranges:
			yield structure_id, start, end, next(self.export_ranges([(start, end)]))

	#
	# statistics functions
	#

	@requires_database
	def get_size(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT COUNT(id) FROM atoms')
		num_atoms = cursor.fetchone()[0]

		cursor.execute('SELECT COUNT(id) FROM structures')
		num_structures = cursor.fetchone()[0]

		return num_atoms, num_structures

	@requires_database
	def get_atom_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT value, COUNT(value) AS total FROM atoms GROUP BY value')
		atom_counts = {v: c for v, c in cursor.fetchall()}

		return atom_counts

	@requires_database
	def get_structure_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT type, COUNT(type) AS total FROM structures GROUP BY type')
		structure_counts = {t: c for t, c in cursor.fetchall()}

		return structure_counts

	@requires_database
	def get_cooccurence(self, source_constraint, target_constraint):
		assert source_constraint.level == target_constraint.level, f"[Error] Source and target constraints must be applied at the same structural level: {source_constraint.level} ≠ {target_constraint.level}."

		# prepare views for easier retrieval
		source_views = construct_views(constraint=source_constraint, view_prefix='source_')
		target_views = construct_views(constraint=target_constraint, view_prefix='target_')

		# select the relevant view based on constraints
		relevant_view = 'filtered_substructures'  # default: relevant structures without constraint
		join_criterion = 'srv.start = trv.start AND srv.end = trv.end'  # default: structures occurring at matching positions

		if (source_constraint.level is not None) and (target_constraint.level is not None):
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
