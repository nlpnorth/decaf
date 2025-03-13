from typing import Optional

class Condition:
	def __init__(self, stype:str, values:Optional[list[str]]=None, literals:Optional[list[str]]=None, match:str='=', min_count:int=1):
		self.type = stype
		self.values = values if values else None
		self.literals = literals if literals else None
		self.match = match
		self.min_count = min_count

	def get_types(self):
		return {self.type}

	def has_literals(self):
		return self.literals is not None

	def to_sql(self, literals=True, column_prefix=''):
		sql = f'{column_prefix}"type={self.type}" IS NOT NULL'
		if self.values is not None:
			query_match = ' OR '.join(f"({column_prefix}\"type={self.type}\" {self.match} '{v}')" for v in self.values)
			sql += f' AND ({query_match})'
		# add literal if not pre-filtering
		if literals and (self.literals is not None):
			query_match = ' OR '.join(f"({column_prefix}literal {self.match} '{l}')" for l in self.literals)
			sql += f' AND ({query_match})'
		return sql

	def to_grouped_sql(self):
		sql = f'SUM(CASE WHEN ({self.to_sql()}) THEN 1 ELSE 0 END) >= {self.min_count}'
		return sql


class Criterion:
	def __init__(self, conditions:list[Condition], operation:str=''):
		self.conditions = conditions
		if len(self.conditions) > 1:
			assert operation != '', f"[Error] Given more than one condition, criteria require a joining operation (e.g., AND, OR)."
		self.operation = operation

	def get_types(self):
		types = set()
		for c in self.conditions:
			types |= c.get_types()
		return types

	def has_literals(self):
		return any(c.has_literals() for c in self.conditions)

	def to_sql(self, literals=True, literals_only=False, column_prefix=''):
		sql_conditions = []

		for condition in self.conditions:
			# check if condition has literal
			if literals_only and (not condition.has_literals()): continue
			# construct sql conditional query
			sql_condition = condition.to_sql(
				literals=literals,
				column_prefix=column_prefix
			)
			if sql_condition:
				sql_conditions.append(f'({sql_condition})')

		sql = f' {self.operation} '.join(sql_conditions)
		return sql

	def to_grouped_sql(self):
		sql = f' {self.operation} '.join(
			f'({c.to_grouped_sql()})' for c in self.conditions
		)
		return sql


class Constraint:
	def __init__(self, criteria:list[Criterion], sequential:bool=False, hierarchy:Optional[list[str]]=None):
		self.criteria = criteria
		self.sequential = sequential
		self.hierarchy = hierarchy

	def has_literals(self):
		return any(c.has_literals() for c in self.criteria)

	def get_types(self):
		types = set()
		for c in self.criteria:
			types |= c.get_types()
		return sorted(types)

	def to_sql(self, literals=True, literals_only=False, column_prefix=''):
		sql_criteria = []

		for criterion in self.criteria:
			sql_criterion = criterion.to_sql(
				literals=literals, literals_only=literals_only,
				column_prefix=column_prefix
			)
			if sql_criterion:
				sql_criteria.append(f'({sql_criterion})')

		sql = f' OR '.join(sql_criteria)
		return sql

	def to_grouped_sql(self):
		sql = f' AND '.join(
			f'({c.to_grouped_sql()})' for c in self.criteria
		)
		return sql
