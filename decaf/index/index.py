import sqlite3

from typing import Union

from decaf.index import Atom, Structure


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

	def add_atoms(self, atoms:list[Atom]):
		cursor = self.db_connection.cursor()

		query = 'INSERT INTO atoms (id, start, end, value) VALUES (?, ?, ?, ?)'
		cursor.executemany(query, [atom.serialize() for atom in atoms])

		self.db_connection.commit()

	def add_structures(self, structures:list[Structure]):
		cursor = self.db_connection.cursor()

		query = 'INSERT INTO structures (id, start, end, value, type, subsumes) VALUES (?, ?, ?, ?, ?, ?)'
		cursor.executemany(query, [structure.serialize() for structure in structures])

		self.db_connection.commit()

	def get_size(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT COUNT(id) FROM atoms')
		num_atoms = cursor.fetchone()[0]

		cursor.execute('SELECT COUNT(id) FROM structures')
		num_structures = cursor.fetchone()[0]

		return num_atoms, num_structures

	def get_atom_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT value, COUNT(value) AS total FROM atoms GROUP BY value')
		atom_counts = {v: c for v, c in cursor.fetchall()}

		return atom_counts

	def get_structure_counts(self):
		cursor = self.db_connection.cursor()

		cursor.execute('SELECT type, COUNT(type) AS total FROM structures GROUP BY type')
		structure_counts = {t: c for t, c in cursor.fetchall()}

		return structure_counts