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

	@requires_database
	def filter(self, constraint, constraint_level = None, output_level = None):
		cursor = self.db_connection.cursor()

		# case: no structural constrain is provided
		if constraint_level is None:
			# retrieves any structures matching the constraint
			query = f'''
			SELECT id, start, end
	        FROM structures
	        WHERE {constraint.to_sql()}'''

			# case: output level differs from the constraint level
			if output_level is not None:
				query = f'''
				SELECT DISTINCT outputs.id, outputs.start, outputs.end
				FROM structures AS outputs
				JOIN ({query}) AS filtered ON (outputs.start <= filtered.start AND outputs.end >= filtered.end)
				WHERE type = "{output_level}"'''

		# case: constraint should be applied within a specific structural level
		else:
			# retrieves structures which contain substructures that match the constraint
			relevant_structures_query = f'''
			WITH relevant_structures AS (
			    SELECT structural_constraint_id, structural_constraint_start, structural_constraint_end, match_id, start, end, type, value
			    FROM (
			        SELECT id AS match_id, start, end, type, value
			        FROM structures
			        WHERE {constraint.to_sql()}
			         )
			    JOIN (
			        SELECT id AS structural_constraint_id, start AS structural_constraint_start, end AS structural_constraint_end
			        FROM structures
			        WHERE type = "{constraint_level}"
			    ) ON (start >= structural_constraint_start AND end <= structural_constraint_end)
			)'''

			# case: output should be at the level of the constraining structure
			filtered_structures_query = f'''
			SELECT structural_constraint_id AS filtered_structural_constraint_id, structural_constraint_start, structural_constraint_end
		    FROM relevant_structures
		    GROUP BY structural_constraint_id
			HAVING ({constraint.to_grouped_sql()})'''

			# case: output should be at the level of the matching substructures
			if output_level is None:
				filtered_structures_query = f'''
				SELECT match_id, start, end
				FROM relevant_structures
				JOIN ({filtered_structures_query})
				ON (filtered_structural_constraint_id = structural_constraint_id)'''
			elif (output_level is not None) and (output_level != constraint_level):
				raise NotImplementedError(f"For structurally constrained queries, output levels besides the constraint or match level are unsupported. Specified output level: '{output_level}'.")

			# complete full query
			query = relevant_structures_query + filtered_structures_query

		# execute constructed query
		cursor.execute(query)

		for structure_id, start, end in cursor.fetchall():
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
