class Condition:
	def __init__(self, stype, values, min_count=0):
		self.type = stype
		self.values = values
		self.min_count = min_count

	def to_sql(self):
		return f'type = "{self.type}" AND value IN {tuple(self.values)}'

	def to_grouped_sql(self):
		query = ' AND '.join(
			f'SUM(CASE WHEN type = "{self.type}" AND value = "{v}" THEN 1 ELSE 0 END) > {self.min_count}'
			for v in self.values
		)
		return query


class Criterion:
	def __init__(self, conditions, operation=''):
		self.conditions = conditions
		if len(self.conditions) > 1:
			assert operation != '', f"[Error] Given more than one condition, criteria require a joining operation (e.g., AND, OR)."
		self.operation = operation

	def to_sql(self):
		conditional = f' {self.operation} '.join(
			f'({c.to_sql()})' for c in self.conditions
		)
		return conditional

	def to_grouped_sql(self):
		conditional = f' {self.operation} '.join(
			f'({c.to_grouped_sql()})' for c in self.conditions
		)
		return conditional