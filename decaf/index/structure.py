from typing import Optional, Union

class Structure:
	def __init__(self, start:int, end:int, stype:str, value:Union[str, None], literals:list, index_id:Optional[int] = None):
		self.start = start
		self.end = end
		self.type = stype
		self.value = value
		self.literals = literals
		self.id = index_id

	def __repr__(self):
		return f'''<Structure (id={self.id}, loc={self.start}-{self.end}, {len(self.literals)} literals):  type='{self.type}', value='{self.value}'>'''

	def __hash__(self):
		return hash(self.serialize())

	def serialize(self):
		return self.id, self.start, self.end, self.type, self.value

