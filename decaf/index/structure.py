from typing import Optional, Union

class Structure:
	def __init__(self, start:int, end:int, value:Union[str, None], stype:str, subsumes:bool, index_id:Optional[int] = None):
		self.start = start
		self.end = end
		self.value = value
		self.type = stype
		self.subsumes = subsumes
		self.id = index_id

	def __repr__(self):
		return f'''<Structure (id={self.id}, loc={self.start}-{self.end}): value='{self.value}', type='{self.type}', subsumes={self.subsumes}>'''

	def serialize(self):
		return self.id, self.start, self.end, self.value, self.type, self.subsumes

