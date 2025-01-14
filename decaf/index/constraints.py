class Condition:
	def __init__(self, stype, values=None, literal=None, min_count=0):
		self.type = stype
		self.values = values
		self.literal = literal
		self.min_count = min_count

	def has_literals(self):
		return self.literal is not None

	def to_sql(self):
		sql = f'type = "{self.type}"'
		if self.values is not None:
			query_set = '(' + ', '.join(f'"{v}"' for v in self.values) + ')'
			sql += f' AND value IN {query_set}'
		# add literal if not pre-filtering
		if self.literal is not None:
			sql += f' AND literal = "{self.literal}"'
		return sql

	def to_grouped_sql(self):
		count_conditions = []
		for v in self.values:
			count_condition = f'type = "{self.type}" AND value = "{v}"'
			if self.literal is not None:
				count_condition += f' AND literal = "{self.literal}"'
			count_condition = f'SUM(CASE WHEN {count_condition} THEN 1 ELSE 0 END) > {self.min_count}'
			count_conditions.append(count_condition)
		sql = ' AND '.join(count_conditions)
		return sql

	def to_prefilter_sql(self, only_literals=False, column_prefix=''):
		if only_literals and (self.literal is None):
			return ''
		query_set = '(' + ', '.join(f'"{v}"' for v in self.values) + ')'
		sql = f'{column_prefix}type = "{self.type}" AND {column_prefix}value IN {query_set}'
		return sql


class Criterion:
	def __init__(self, conditions, operation=''):
		self.conditions = conditions
		if len(self.conditions) > 1:
			assert operation != '', f"[Error] Given more than one condition, criteria require a joining operation (e.g., AND, OR)."
		self.operation = operation

	def has_literals(self):
		return any(c.has_literals() for c in self.conditions)

	def to_sql(self):
		sql = f' {self.operation} '.join(
			f'({c.to_sql()})' for c in self.conditions
		)
		return sql

	def to_grouped_sql(self):
		sql = f' {self.operation} '.join(
			f'({c.to_grouped_sql()})' for c in self.conditions
		)
		return sql

	def to_prefilter_sql(self, only_literals=False, column_prefix=''):
		# if pre-filtering, relax conjunctive requirements to OR to retrieve all potentially relevant matches
		sql = f' OR '.join(
			f'({c.to_prefilter_sql(only_literals=only_literals, column_prefix=column_prefix)})'
			for c in self.conditions
			if c.to_prefilter_sql(only_literals=only_literals, column_prefix=column_prefix)
		)
		return sql