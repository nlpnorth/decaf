import argparse
import re
from operator import index

from typing import Optional

from decaf.index import Atom, Structure, DecafIndex

from conllu import TokenList, Token, parse_incr


#
# UD constants
#

# metadata to be carried over across sentences
# format: 'regex' -> 'index field' / None (keep as is)
METADATA_CARRYOVER = {
	r'^newdoc( id)?': 'document',  # indicates start of new document (optionally with ID)
	r'^newpar( id)?': 'paragraph',  # indicates start of new document (optionally with ID)
	r'meta::.+': None  # GUM document-level metadata field (e.g., 'meta::dateCollected')
}


#
# helper functions
#

def parse_arguments():
	parser = argparse.ArgumentParser(description="UD Importer")
	parser.add_argument('--input', required=True, help='path to UD treebank in CoNLL-U format')
	parser.add_argument('--output', required=True, help='path to output SQLite index')
	return parser.parse_args()


def get_carryover_field(field):
	for carryover_pattern, index_field in METADATA_CARRYOVER.items():
		# check if metadata field matches carryover field pattern
		if re.match(carryover_pattern, field) is None:
			continue
		# check if field requires name conversion
		if index_field is None:
			return field
		else:
			return index_field
	# return None is examined field is not for carryover
	return None


#
# parser functions
#

def parse_token(token:Token, cursor_idx:int, trailing_space:Optional[bool] = None) -> tuple[list[Atom], list[Structure]]:
	atoms, structures = [], []

	# create atoms from characters
	for character_idx, character in enumerate(token['form']):
		atoms.append(
			Atom(start=cursor_idx + character_idx, end=cursor_idx + character_idx + 1, value=character)
		)
	trailing_space = True if trailing_space is None else trailing_space

	# create structures from UD's token-level annotations
	# https://universaldependencies.org/format.html
	start_idx, end_idx = cursor_idx, cursor_idx + len(token['form'])
	for annotation in token:
		# token's surface form
		if annotation == 'form':
			structures.append(
				Structure(
					start=start_idx, end=end_idx,
					value=None, stype='token',  # literal value as it's constructed from the atoms
					subsumes=True  # token subsumes its characters
				)
			)
		# skip redundant annotations (UD ID, dep-tuple)
		elif annotation in {'id', 'deps'}:
			continue
		# skip empty annotation fields
		elif token[annotation] is None:
			continue
		# split multi-value annotation fields into individual structures
		elif type(token[annotation]) is dict:
			for misc_annotation, misc_value in token[annotation].items():
				structures.append(
					Structure(
						start=start_idx, end=end_idx,
						value=misc_value, stype=misc_annotation,
						subsumes=False
					)
				)
			# check for SpaceAfter=No MISC annotation
			if ('SpaceAfter' in token[annotation]) and (token[annotation]['SpaceAfter'] == 'No'):
				# prevent adding trailing space
				trailing_space = False
		# all other annotations are stored as token-level structures
		else:
			structures.append(
				Structure(
					start=start_idx, end=end_idx,
					value=token[annotation], stype=annotation,
					subsumes=False  # token-level annotations are not transitive w.r.t. individual characters
				)
			)

	if trailing_space:
		atoms.append(Atom(start=end_idx, end=end_idx + 1, value=' '))

	return atoms, structures


def parse_sentence(sentence:TokenList, cursor_idx:int) -> tuple[list[Atom], list[Structure], dict]:
	atoms, structures = [], []
	carryover = {}

	# parse tokens in sentence
	token_cursor_idx = int(cursor_idx)
	multitoken_end = None
	multitoken_space = None
	for token_idx, token in enumerate(sentence):
		# check for multi-tokens (e.g. "It's" -> "It 's"), identified by ID with range (e.g., '3-4')
		if type(token['id']) is tuple:
			multitoken_end = token['id'][-1]
			if (token['misc'] is not None) and ('SpaceAfter' in token['misc']) and (token['misc']['SpaceAfter'] == 'No'):
				multitoken_space = False
			continue
		# trailing space behaviour follows default, except within and at the end of multi-tokens
		trailing_space = None
		if multitoken_end is not None:
			trailing_space = False
			if token['id'] >= multitoken_end:
				trailing_space = multitoken_space
				multitoken_end, multitoken_space = None, None

		# process token
		token_atoms, token_structures = parse_token(token, token_cursor_idx, trailing_space=trailing_space)
		atoms += token_atoms
		structures += token_structures
		token_cursor_idx += len(token_atoms)

	# create structures from UD's sentence-level annotations
	start_idx, end_idx = cursor_idx, token_cursor_idx
	# sentence structure itself
	structures.append(
		Structure(
			start=start_idx, end=end_idx,
			value=None, stype='sentence',
			subsumes=True  # sentence subsumes its characters
		)
	)
	# sentence metadata
	for meta_field, meta_value in sentence.metadata.items():
		# extract special carryover metadata ('newdoc id', 'newpar id', 'newpar', ...)
		carryover_field = get_carryover_field(meta_field)
		if carryover_field is not None:
			carryover[carryover_field] = (meta_value, start_idx)
			continue
		# skip redundant UD field (text)
		if meta_field == 'text':
			continue
		# all other metadata are stored as sentence-level structures
		structures.append(
			Structure(
				start=start_idx, end=end_idx,
				value=meta_value, stype=meta_field,
				subsumes=True  # sentence subsumes its characters
			)
		)

	return atoms, structures, carryover


def parse_carryover(carryover:dict, next_carryover:dict, cursor_idx:int) -> tuple[dict, list[Structure]]:
	carryover_structures = []

	# check if paragraph (or document) changed
	if ('paragraph' in next_carryover) or ('document' in next_carryover):
		paragraph_id, paragraph_start_idx = carryover['paragraph']
		carryover_structures.append(
			Structure(
				start=paragraph_start_idx, end=cursor_idx,
				value=None, stype='paragraph',
				subsumes=True  # document subsumes its sentences
			)
		)
		if paragraph_id:
			carryover_structures.append(
				Structure(
					start=paragraph_start_idx, end=cursor_idx,
					value=paragraph_id, stype='paragraph_id',
					subsumes=True  # document subsumes its sentences
				)
			)
		next_carryover['paragraph'] = next_carryover.get('paragraph', (None, cursor_idx))

	# check if document changed
	if 'document' in next_carryover:
		# create document-level structures and flush metadata
		for co_field, (co_value, co_start) in carryover.items():
			# create separate document and document ID structures
			if co_field == 'document':
				carryover_structures.append(
					Structure(
						start=co_start, end=cursor_idx,
						value=None, stype='document',
						subsumes=True  # document subsumes its sentences
					)
				)
				co_field = 'document_id'

			# skip re-processing of paragraph metadata
			if co_field == 'paragraph':
				continue

			# add remaining document-level metadata
			carryover_structures.append(
				Structure(
					start=co_start, end=cursor_idx,
					value=co_value, stype=co_field,
					subsumes=True  # document subsumes its sentences
				)
			)
		# start from new carryover data
		carryover = next_carryover

	return carryover, carryover_structures


#
# main
#

def main():
	args = parse_arguments()
	print("="*13)
	print("📥️ UD Import")
	print("="*13)

	# set up associated DECAF index
	decaf_index = DecafIndex(db_path=args.output)
	print(f"Connected to DECAF index at '{args.output}'.")

	print(f"Loading UD treebank from '{args.input}'...")
	with open(args.input) as fp, decaf_index as di:
		num_atoms, num_structures = di.get_size()
		cursor_idx = 0  # initialize character-level dataset cursor
		carryover = {}  # initialize cross-sentence carryover metadata (e.g., document/paragraph info)

		# iterate over sentences
		for sentence_idx, sentence in enumerate(parse_incr(fp)):
			print(f"\x1b[1K\r[{sentence_idx + 1}] Building index...", end='', flush=True)
			cur_atoms, cur_structures, cur_carryover = parse_sentence(sentence, cursor_idx)

			# process carryover metadata
			if sentence_idx == 0:
				carryover = cur_carryover  # first carryover metadata is always retained
			else:
				carryover, carryover_structures = parse_carryover(carryover, cur_carryover, cursor_idx)
				cur_structures += carryover_structures

			# insert parsed atoms and structures into index
			di.add_atoms(atoms=cur_atoms)
			di.add_structures(structures=cur_structures)

			cursor_idx += len(cur_atoms)  # increment character-level cursor by number of atoms (i.e., characters)

		# process final carryover structures
		_, carryover_structures = parse_carryover(carryover, {'document': ('end', -1), 'paragraph': ('end', -1)}, cursor_idx)
		di.add_structures(structures=carryover_structures)

		# compute number of added structures
		new_num_atoms, new_num_structures = di.get_size()
		print(f"\x1b[1K\rBuilt index with {new_num_atoms - num_atoms} atoms and {new_num_structures - num_structures} structures for {sentence_idx + 1} sentences from '{args.input}'.")

		# print statistics
		atom_counts = di.get_atom_counts()
		print(f"Atom Statistics ({sum(atom_counts.values())} total; {len(atom_counts)} unique):")
		for atom, count in sorted(atom_counts.items(), key=lambda i: i[1], reverse=True):
			print(f"  '{atom}': {count} occurrences")

		structure_counts = di.get_structure_counts()
		print(f"Structure Statistics ({sum(structure_counts.values())} total; {len(structure_counts)} unique):")
		for structure, count in sorted(structure_counts.items(), key=lambda i: i[1], reverse=True):
			print(f"  '{structure}': {count} occurrences")

	print(f"Saved updated DECAF index to '{args.output}'.")


if __name__ == '__main__':
	main()