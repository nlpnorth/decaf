from typing import Optional

class Atom:
	def __init__(self, start:int, end:int, value:str, index_id:Optional[int] = None):
		self.start = start
		self.end = end
		self.value = value
		self.id = index_id

	def __repr__(self):
		return f'''<Atom (id={self.id}, loc={self.start}-{self.end}): value='{self.value}'>'''

	def serialize(self):
		return self.id, self.start, self.end, self.value
