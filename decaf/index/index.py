import sqlite3

from typing import Union

from decaf.index import Atom, Structure

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

	@staticmethod
	def _construct_views(constraint, constraint_level):
		#
		# SQL Sub-queries Mega Block
		#

		views = {}

		# view containing all potentially relevant structures (w/o literals)
		# e.g., all upos=(NOUN|ADJ)
		relevant_structures_view = f'''
		SELECT id AS match_id, start, end, type, value
        FROM structures
        WHERE {constraint.to_prefilter_sql()}
		'''

		# view containing all potentially relevant structures (w/o literals) which are contained within the specified parent constraint level
		# e.g., all structures subsumed by 'sentence', which contain at least one upos=(NOUN|ADJ)
		if constraint_level is not None:
			relevant_structures_view = f'''
		    SELECT structural_constraint_id, structural_constraint_start, structural_constraint_end, match_id, start, end, type, value
		    FROM ({relevant_structures_view})
		    JOIN (
		        SELECT id AS structural_constraint_id, start AS structural_constraint_start, end AS structural_constraint_end
		        FROM structures
		        WHERE type = "{constraint_level}"
		        )
		    ON (start >= structural_constraint_start AND end <= structural_constraint_end)'''

		views['relevant_structures'] = relevant_structures_view

		# set the relevant view depending on whether literals are involved or not
		relevant_view = 'relevant_structures'

		# views for handling constraints with literals
		if constraint.has_literals():
			# view containing all potentially relevant structures + their literals for structures for which literals were queried (otherwise NULL)
			# e.g., all relevant structures + literals for all upos=ADJ, but not for upos=NULL
			relevant_literals_view = f'''
			SELECT {'relevant.structural_constraint_id AS structural_constraint_id, relevant.structural_constraint_start AS structural_constraint_start, relevant.structural_constraint_end AS structural_constraint_end,' if constraint_level is not None else ''} relevant.match_id AS match_id, relevant.start AS start, relevant.end AS end, relevant.type AS type, relevant.value AS value, literal
		    FROM
		        relevant_structures AS relevant
		    LEFT JOIN (
		        SELECT {'structural_constraint_id, structural_constraint_start, structural_constraint_end,' if constraint_level is not None else ''} relevant_structures.match_id AS id, relevant_structures.start AS start, relevant_structures.type AS type, relevant_structures.value AS value, GROUP_CONCAT(atoms.value, '') as literal
		        FROM
		            relevant_structures
		        JOIN
		            atoms
		        ON (atoms.start >= relevant_structures.start AND atoms.end <= relevant_structures.end AND ({constraint.to_prefilter_sql(only_literals=True, column_prefix='relevant_structures.')}))
		        GROUP BY relevant_structures.match_id) AS literals
		    ON (relevant.match_id = literals.id)'''

			views['relevant_literals'] = relevant_literals_view
			relevant_view = 'relevant_literals'

		# views for handling constraint application at specific structural levels
		if constraint_level is not None:
			# view containing all parent structures, which fulfill all substructural constraints
			# e.g., all sentences containing at least one upos=(ADJ|NOUN) each
			filtered_structures_view = f'''
			SELECT structural_constraint_id, structural_constraint_start, structural_constraint_end
	        FROM {relevant_view}
	        GROUP BY structural_constraint_id
	        HAVING ({constraint.to_grouped_sql()})'''

			views['filtered_structures'] = filtered_structures_view

			# view containing all substructures which matched the criterion within the parent structural constraint
			# e.g., all upos=(ADJ|NOUN) within all sentences, that contain at least one of each
			filtered_substructures_view = f'''
			SELECT match_id, start, end
		    FROM 
		        relevant_structures AS relevant
		    JOIN
		        filtered_structures AS filtered
		    ON (filtered.structural_constraint_id = relevant.structural_constraint_id)'''

			views['filtered_substructures'] = filtered_substructures_view

		# construct query prefix with all available views
		views = 'WITH ' + '\n, '.join(f'{name} AS ({definition})' for name, definition in views.items()) + '\n'

		return views

	def _construct_filter_query(self, constraint, constraint_level, output_level):
		#
		# SQL Query Construction Logic
		#
		views = self._construct_views(constraint=constraint, constraint_level=constraint_level)
		relevant_view = 'relevant_literals' if constraint.has_literals() else 'relevant_structures'

		# case: no structural constraint is provided
		if constraint_level is None:
			# case: retrieve all matching structures
			query = f'SELECT match_id, start, end FROM {relevant_view} WHERE {constraint.to_sql()}'

			# case: output level differs from the level of the matched constraints
			if output_level is not None:
				query = f'''
				SELECT DISTINCT outputs.id, outputs.start, outputs.end
				FROM structures AS outputs
				JOIN ({query}) AS filtered ON (outputs.start <= filtered.start AND outputs.end >= filtered.end)
				WHERE outputs.type = "{output_level}"'''

		# case: constraint should be applied within a specific structural level
		else:
			# case: output should be at the level of the constraining structure
			query = 'SELECT * FROM filtered_structures'

			# case: output should be at the level of the matching substructures
			if output_level is None:
				query = 'SELECT * FROM filtered_substructures'

			# case: output level does not match the constraint level
			elif (output_level is not None) and (output_level != constraint_level):
				raise NotImplementedError(
					f"For structurally constrained queries, output levels besides the constraint or match level are unsupported. Specified output level: '{output_level}'.")

		# prefix available views to query
		query = views + query

		return query

	@requires_database
	def get_filter_ranges(self, constraint, constraint_level, output_level):
		cursor = self.db_connection.cursor()

		# execute constructed query
		query = self._construct_filter_query(
			constraint=constraint,
			constraint_level=constraint_level,
			output_level=output_level
		)
		cursor.execute(query)

		return cursor.fetchall()

	@requires_database
	def filter(self, constraint, constraint_level = None, output_level = None):
		filter_ranges = self.get_filter_ranges(
			constraint=constraint,
			constraint_level=constraint_level,
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
