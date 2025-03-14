import os
import sqlite3

from importlib import resources

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
		if self.connection is None:
			raise RuntimeError(f"The {func.__name__} function must be called within an active database connection context.")
		return func(self, *args, **kwargs)

	return database_function


#
# Main DECAF Index
#

class DecafIndex:
	def __init__(self, index_path):
		self.index_path = index_path
		self.shards = DecafIndex.load_shards(self.index_path)
		self.connection = None
		self.connected_shard = None

	def __repr__(self):
		return f'''<DecafIndex: {len(self.shards)} shard(s), {'dis' if self.connection is None else ''}connected>'''

	def __enter__(self):
		self.connect()
		return self

	def __exit__(self, exception_type, exception_value, exception_traceback):
		self.disconnect()

	def initialize(self):
		if len(self.shards) < 1:
			self.add_shard()

	#
	# database communication
	#

	def connect(self, shard=0):
		self.connection =  sqlite3.connect(self.shards[shard])
		self.connected_shard = shard
		return self.connection

	def disconnect(self):
		if self.connection is not None:
			self.commit()
			self.connection.close()
			self.connection = None
			self.connected_shard = None

	def connections(self):
		for shard_idx in range(len(self.shards)):
			self.disconnect()
			yield self.connect(shard=shard_idx)
		self.disconnect()

	@requires_connection
	def commit(self):
		self.connection.commit()

	#
	# sharding
	#

	@staticmethod
	def load_shards(index_path):
		shards = []
		shard_file = os.path.join(index_path, f'shard-{len(shards)}.decaf')
		while os.path.exists(shard_file):
			shards.append(shard_file)
			shard_file = os.path.join(index_path, f'shard-{len(shards)}.decaf')
		return shards

	def add_shard(self):
		# disconnect from previous shard
		self.disconnect()
		# check if index directory exists
		if not os.path.exists(self.index_path):
			os.mkdir(self.index_path)
		# create new database file
		shard_idx = len(self.shards)
		shard_file = os.path.join(self.index_path, f'shard-{shard_idx}.decaf')
		self.shards.append(shard_file)
		# connect to new shard
		self.connect(shard=shard_idx)
		# initialize shard from default schema
		cursor = self.connection.cursor()
		with resources.open_text('decaf.config', 'schema.sql') as fp:
			schema = fp.read()
		cursor.executescript(schema)
		self.commit()

	#
	# import functions
	#

	def add(self, literals:list[Literal], structures:list[Structure], hierarchies:list[tuple[Structure,Structure]]):
		# connect to last shard
		if (self.connection is None) or (self.connected_shard != len(self.shards) - 1):
			assert len(self.shards) > 0, "[Error] Cannot write to index without shards."
			self.connect(shard=len(self.shards)-1)

		# insert literals into index (this updates the associated literals' index IDs)
		self._add_literals(literals=literals)
		# insert structures into index (associate structures and previously initialized literal IDs)
		self._add_structures(structures=structures)
		# insert hierarchies into index (based on previously initialized structure IDs)
		self._add_hierarchies(hierarchies=hierarchies)

	@requires_connection
	def _add_literals(self, literals:list[Literal]) -> list[Literal]:
		cursor = self.connection.cursor()

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
	def _add_structures(self, structures:list[Structure]) -> list[Structure]:
		cursor = self.connection.cursor()

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
	def _add_hierarchies(self, hierarchies:list[tuple[Structure,Structure]]):
		cursor = self.connection.cursor()

		assert all((parent.id is not None) and (child.id is not None) for parent, child in hierarchies), f"[Error] Please make sure to add all structures to the index before adding the corresponding hierarchies."

		query = 'INSERT INTO hierarchical_structures (parent, child) VALUES (?, ?)'
		cursor.executemany(query, [(parent.id, child.id) for parent, child in hierarchies])

	#
	# export functions
	#

	@requires_connection
	def export_ranges(self, ranges):
		cursor = self.connection.cursor()

		for start, end in ranges:
			query = 'SELECT GROUP_CONCAT(value, "") as export FROM literals WHERE start >= ? AND end <= ?'
			cursor.execute(query, (start, end))
			yield cursor.fetchone()[0]

	@requires_connection
	def export_masked(self, mask_ranges):
		num_literals, _, _ = self.get_size()
		mask_ranges += [(None, num_literals, num_literals)]

		cursor = self.connection.cursor()

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
		cursor = self.connection.cursor()

		# execute constructed query
		query = self._construct_filter_query(
			constraint=constraint,
			output_level=output_level
		)
		cursor.execute(query)

		return cursor.fetchall()

	def filter(self, constraint, output_level='structures'):
		for shard_idx, connection in enumerate(self.connections()):
			filter_ranges = self.get_filter_ranges(
				constraint=constraint,
				output_level=output_level
			)
			for structure_id, start, end in filter_ranges:
				yield (shard_idx, structure_id), start, end, next(self.export_ranges([(start, end)]))

	def mask(self, constraint, mask_level='structures'):
		for connection in self.connections():
			filter_ranges = self.get_filter_ranges(
				constraint=constraint,
				output_level=mask_level
			)
			for output in self.export_masked(mask_ranges=filter_ranges):
				yield output

	#
	# statistics functions
	#

	def get_size(self):
		num_literals, num_structures, num_hierarchies = 0, 0, 0

		for connection in self.connections():
			cursor = connection.cursor()

			cursor.execute('SELECT COUNT(id) FROM literals')
			num_literals += cursor.fetchone()[0]

			cursor.execute('SELECT COUNT(id) FROM structures')
			num_structures += cursor.fetchone()[0]

			cursor.execute('SELECT COUNT(parent) FROM hierarchical_structures')
			num_hierarchies += cursor.fetchone()[0]

		return num_literals, num_structures, num_hierarchies

	def get_literal_counts(self):
		literal_counts = {}

		for connection in self.connections():
			cursor = connection.cursor()
			cursor.execute('SELECT value, COUNT(value) AS total FROM literals GROUP BY value')
			for literal, count in cursor.fetchall():
				literal_counts[literal] = literal_counts.get(literal, 0) + count

		return literal_counts

	def get_structure_counts(self):
		structure_counts = {}

		for connection in self.connections():
			cursor = connection.cursor()
			cursor.execute('SELECT type, COUNT(type) AS total FROM structures GROUP BY type')
			for structure, count in cursor.fetchall():
				structure_counts[structure] = structure_counts.get(structure, 0) + count

		return structure_counts

	def get_cooccurence(self, source_constraint, target_constraint):
		cooccurrence = pd.DataFrame(dtype=int)

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

		# iterate over shards
		for connection in self.connections():
			shard_co = pd.read_sql_query(query, connection)

			# pivot co-occurrence rows to become a matrix
			shard_co = shard_co.pivot(
			    index='sources',
			    columns='targets',
			    values='frequency'
			).fillna(0)

			# merge across shards
			shard_co = shard_co.reindex(columns=set().union(cooccurrence.columns, shard_co.columns), fill_value=0)  # fill in missing columns
			cooccurrence = cooccurrence.add(shard_co, fill_value=0)

		cooccurrence = cooccurrence.astype(int)
		return cooccurrence
