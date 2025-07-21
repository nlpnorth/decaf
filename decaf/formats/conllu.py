import re

from conllu import TokenList, TokenTree, Token, parse_incr

from decaf.index import Literal, Structure
from decaf.formats.parser import FormatParser


# metadata to be carried over across sentences
# format: 'regex' -> 'index field' / None (keep as is)
CONLLU_METADATA_CARRYOVER = {
	r'^newdoc( id)?': 'document',  # indicates start of new document (optionally with ID)
	r'^newpar( id)?': 'paragraph',  # indicates start of new document (optionally with ID)
	r'meta::.+': None  # GUM document-level metadata field (e.g., 'meta::dateCollected')
}


class ConlluBatcher:
    def __init__(self, file, start=0, end=float('inf')):
        self.file = file
        self.start = start
        self.end = end
        self._file_pointer = None

    def __enter__(self):
        self._file_pointer = open(self.file)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_pointer.close()

    @staticmethod
    def get_size(file):
        with open(file) as fp:
            num_sentences = sum(1 for line in fp if line.startswith('1\t'))
        return num_sentences

    def get_boundary(self, sentence):
        for field in ['newdoc', 'newdoc id']:
            if field in sentence.metadata:
                return 'document'
        return None

    def get_batches(self, batch_size):
        assert self._file_pointer is not None, f"[Error] ConlluBatcher must be used within a context manager."

        batch = []

        # iterate over all sentences
        in_boundary = False  # flag to check whether sentence falls within structural boundary
        for sentence_idx, sentence in enumerate(parse_incr(self._file_pointer)):
            # seek ahead until offset (cannot call file.seek() because parser calls next())
            if (sentence_idx < self.start) or (sentence_idx > self.end):
                continue

            # check if batch has reached target size
            batch_complete = False
            if len(batch) >= batch_size:
                # check for new document boundary
                if self.get_boundary(sentence):
                    # if previous batch is not empty
                    if len(batch) > 0:
                        batch_complete = True
                    # mark sentence as being within document
                    in_boundary = True

                # if sentence does not fall within a document boundary, batch is complete at target size
                if not in_boundary:
                    batch_complete = True

            # append batch if complete
            if batch_complete:
                # export previous batch and start new one
                yield batch
                batch = []

            # append sentence to batch
            batch.append(sentence)

        # yield final batch
        yield batch


class ConlluParser(FormatParser):
    def __init__(self, literal_level:str, force_alignment:bool, sentence_terminator:str):
        super().__init__()
        self.literal_level = literal_level
        self.force_alignment = force_alignment
        self.sentence_terminator = sentence_terminator

    def get_carryover_field(self, field):
        for carryover_pattern, index_field in CONLLU_METADATA_CARRYOVER.items():
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

    def parse_token(self, token: Token, cursor_idx: int, literal_level: str) -> tuple[
        list[Literal], list[Structure], list[tuple[Structure, Structure]]]:
        literals, structures, hierarchies = [], [], []

        # create literals from characters
        if literal_level == 'character':
            for character_idx, character in enumerate(token['form']):
                literals.append(
                    Literal(start=cursor_idx + character_idx, end=cursor_idx + character_idx + 1, value=character)
                )
        # create literals from tokens
        elif literal_level == 'token':
            literals.append(Literal(start=cursor_idx, end=cursor_idx + len(token['form']), value=token['form']))
        else:
            raise ValueError(f"Unknown literal level: '{literal_level}'.")

        # create structures from UD's token-level annotations
        # https://universaldependencies.org/format.html
        start_idx, end_idx = cursor_idx, cursor_idx + len(token['form'])
        # token's surface form
        token_structure = Structure(
            start=start_idx, end=end_idx,
            value=None, stype='token',  # value=None as it's constructed from its literals
            literals=[l for l in literals]
        )
        structures.append(token_structure)
        # create structures from other token-level annotations
        for annotation in token:
            # skip redundant annotations (UD ID, dep-tuple)
            if annotation in {'id', 'deps', 'form'}:
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
                            literals=[l for l in literals]
                        )
                    )
                    hierarchies.append((token_structure, structures[-1]))
            # all other annotations are stored as token-level structures
            else:
                structures.append(
                    Structure(
                        start=start_idx, end=end_idx,
                        value=token[annotation], stype=annotation,
                        literals=[l for l in literals]
                    )
                )
                hierarchies.append((token_structure, structures[-1]))

        return literals, structures, hierarchies

    def parse_dependencies(self, tree: TokenTree, token_structures: dict[int, Structure]):
        structures, hierarchies = [], []

        relation = tree.token['deprel']
        token_id = tree.token['id']
        literals = [l for l in token_structures[token_id].literals]
        start_idx, end_idx = token_structures[token_id].start, token_structures[token_id].end

        # recursively process child nodes
        children = []  # store direct children for hierarchy
        for child in tree.children:
            child_structures, child_hierarchies, child_literals = self.parse_dependencies(tree=child,
                                                                                     token_structures=token_structures)
            children.append(child_structures[0])
            structures += child_structures
            hierarchies += child_hierarchies
            literals += child_literals
            start_idx = min(start_idx, token_structures[child.token['id']].start)
            end_idx = max(end_idx, token_structures[child.token['id']].end)

        # append parent structure
        dependency = Structure(
            start=start_idx, end=end_idx,
            value=relation, stype='dependency',
            literals=[l for l in literals]
        )
        hierarchies += \
            [(dependency, token_structures[token_id])] + \
            [(dependency, child) for child in children]
        structures = [dependency] + structures

        return structures, hierarchies, literals

    def parse_carryover(
            self,
            carryover:dict, next_carryover:dict,
            literals:dict[str, list[Literal]], next_literals:list[Literal],
            sentences:dict[str, list[Structure]], next_sentence:Structure,
            cursor_idx:int
    ) -> tuple[dict, dict[str, list[Literal]], dict[str, list[Structure]], list[Structure], list[tuple[Structure,Structure]]]:
        output_structures = []
        output_hierarchies = []

        # check if paragraph (or document) changed
        if ('paragraph' in next_carryover) or ('document' in next_carryover):
            # store previous paragraph information
            if 'paragraph' in carryover:
                paragraph_id, paragraph_start_idx = carryover['paragraph']
                paragraph = Structure(
                        start=paragraph_start_idx, end=cursor_idx,
                        value=None, stype='paragraph',
                        literals=[l for l in literals['paragraph']]
                    )
                output_structures.append(paragraph)

                if paragraph_id:
                    output_structures.append(
                        Structure(
                            start=paragraph_start_idx, end=cursor_idx,
                            value=paragraph_id, stype='paragraph_id',
                            literals=[l for l in literals['paragraph']]
                        )
                    )

                # add hierarchical structures at paragraph-level
                output_hierarchies += [
                    (paragraph, sentence_structure)
                    for sentence_structure in sentences['paragraph']
                ]

            # reset parameter-level carryover
            next_carryover['paragraph'] = next_carryover.get('paragraph', (None, cursor_idx))
            literals['paragraph'] = []
            sentences['paragraph'] = []

        # check if document changed
        if 'document' in next_carryover:
            document = None

            # create document-level structures and flush metadata
            if 'document' in carryover:
                for co_field, (co_value, co_start) in carryover.items():
                    # create separate document and document ID structures
                    if co_field == 'document':
                        document =Structure(
                                start=co_start, end=cursor_idx,
                                value=None, stype='document',
                                literals=[l for l in literals['document']]
                            )
                        co_field = 'document_id'

                    # skip re-processing of paragraph metadata
                    if co_field == 'paragraph':
                        continue

                    # add remaining document-level metadata
                    output_structures.append(
                        Structure(
                            start=co_start, end=cursor_idx,
                            value=co_value, stype=co_field,
                            literals=[l for l in literals['document']]
                        )
                    )

            # add document-level hierarchical structures
            if document is not None:
                output_hierarchies += [
                    (document, document_structure)
                    for document_structure in output_structures
                ]
                output_hierarchies += [
                    (document, sentence_structure)
                    for sentence_structure in sentences['document']
                ]
                # add document to output structures
                output_structures = [document] + output_structures

            # reset all carryover data
            carryover = next_carryover
            literals = {s:[] for s in next_carryover}
            sentences = {s:[] for s in next_carryover}

        # keep track of literals and sentences that are part of an ongoing carryover structure
        literals = {s: v + next_literals for s, v in literals.items()}
        sentences = {s: v + [next_sentence] for s, v in sentences.items()}

        return carryover, literals, sentences, output_structures, output_hierarchies

    def parse_sentence(self, sentence:TokenList, cursor_idx:int, literal_level:str, force_alignment:bool=False, sentence_terminator:str='') -> tuple[list[Literal], list[Structure], list[tuple[Structure,Structure]], dict]:
        literals, structures, hierarchies = [], [], []
        carryover = {}

        # parse tokens in sentence
        sentence_tokens = [token for token in sentence if type(token['id']) is not tuple]  # remove multi-tokens (e.g. "It's" -> "It 's"), identified by ID with range (e.g., '3-4')
        text_cursor_idx = 0  # position within sentence
        tokens_by_id = {}
        for token_idx, token in enumerate(sentence_tokens):
            # process token
            token_literals, token_structures, token_hierarchies = self.parse_token(
                token, cursor_idx + text_cursor_idx,
                literal_level=literal_level
            )
            literals += token_literals
            structures += token_structures
            hierarchies += token_hierarchies
            tokens_by_id[token['id']] = token_structures[0]

            # case: force alignment between tokens and original sentence text
            if force_alignment:
                # search for current token
                sentence_continuation = sentence.metadata['text'][text_cursor_idx:]
                token_pattern = '^(' + r'\s*'.join(re.escape(char) for char in token['form']) + ')(\s*)'
                token_match = re.match(token_pattern, sentence_continuation)
                if token_match is None:
                        raise ValueError(
                            f"[Error] Could not find token '{token}' in '{sentence_continuation}'.\n"
                            f"  * sentence: {sentence}\n"
                            f"  * token: '{token}'"
                        )
                # increment text cursor position to after current token
                text_cursor_idx += len(token_match[1])

                # add intermediate whitespaces
                intermediate_literal = token_match[2]

                if len(intermediate_literal) > 0:
                    if (len(intermediate_literal) > 5) and (not re.match(r'\s+', intermediate_literal)):
                        print(f"\n[Warning] Overly long intermediate literal detected at character {cursor_idx}:")
                        print(f"  * sentence: {sentence}")
                        print(f"  * token: '{token}'")
                        print(f"  * intermediate: '{intermediate_literal}'")
                    literals.append(
                        Literal(
                            start=cursor_idx + text_cursor_idx - len(intermediate_literal),
                            end=cursor_idx + text_cursor_idx,
                            value=intermediate_literal)
                    )
                    text_cursor_idx += len(intermediate_literal)
            # case: treat tokens as ground truth
            else:
                # increment cursor by length of token
                text_cursor_idx += sum(len(tl.value) for tl in token_literals)
                # add default whitespace
                literals.append(
                    Literal(
                        start=cursor_idx + text_cursor_idx,
                        end=cursor_idx + text_cursor_idx + 1,
                        value=' ')
                )
                text_cursor_idx += 1

        # add sentence terminator
        if sentence_terminator:
            literals.append(
                Literal(
                    start=cursor_idx + text_cursor_idx,
                    end=cursor_idx + text_cursor_idx + 1,
                    value=sentence_terminator)
            )
            text_cursor_idx += 1

        # create hierarchical dependency structures
        dependency_structures, dependency_hierarchies, _ = self.parse_dependencies(
            tree=sentence.to_tree(),
            token_structures=tokens_by_id
        )
        structures = dependency_structures + structures
        hierarchies = dependency_hierarchies + hierarchies

        # create structures from UD's sentence-level annotations
        start_idx, end_idx = cursor_idx, cursor_idx + text_cursor_idx
        # sentence structure itself
        sentence_structure = Structure(
            start=start_idx, end=end_idx,
            value=None, stype='sentence',
            literals=[l for l in literals]
        )
        sentence_structures = [sentence_structure]
        # sentence metadata
        for meta_field, meta_value in sentence.metadata.items():
            # extract special carryover metadata ('newdoc id', 'newpar id', 'newpar', ...)
            carryover_field = self.get_carryover_field(meta_field)
            if carryover_field is not None:
                carryover[carryover_field] = (meta_value, start_idx)
                continue
            # skip redundant UD field (text)
            if meta_field == 'text':
                continue
            # all other metadata are stored as sentence-level structures
            sentence_structures.append(
                Structure(
                    start=start_idx, end=end_idx,
                    value=meta_value, stype=meta_field,
                    literals=[l for l in literals]
                )
            )

        # establish sentence-level hierarchies
        hierarchies += \
                [(sentence_structure, token) for token in tokens_by_id.values()] + \
                [(sentence_structure, dependency) for dependency in dependency_structures] + \
                [(sentence_structure, sentence_annotation) for sentence_annotation in sentence_structures[1:]]

        structures = sentence_structures + structures

        return literals, structures, hierarchies, carryover

    def parse(self, sentences:list[TokenList], cursor_idx:int=0):
        literals, structures, hierarchies = [], [], []

        cursor_idx = int(cursor_idx) if cursor_idx else 0  # initialize character-level dataset cursor
        carryover = {}  # initialize cross-sentence carryover metadata (e.g., document/paragraph info)
        carryover_literals = {}  # initialize cross-sentence carryover literals for paragraphs and documents
        carryover_sentences = {}  # initialize carryover sentences for paragraphs and documents

        # iterate over sentences
        for sentence in sentences:
            cur_literals, cur_structures, cur_hierarchies, cur_carryover = self.parse_sentence(
                sentence, cursor_idx,
                literal_level=self.literal_level,
                force_alignment=self.force_alignment,
                sentence_terminator=self.sentence_terminator
            )
            cur_sentence = cur_structures[0]
            literals += cur_literals
            structures += cur_structures
            hierarchies += cur_hierarchies

            # process carryover metadata
            carryover, carryover_literals, carryover_sentences, new_structures, new_hierarchies = self.parse_carryover(
                carryover, cur_carryover,
                carryover_literals, cur_literals,
                carryover_sentences, cur_sentence,
                cursor_idx
            )
            structures += new_structures
            hierarchies += new_hierarchies

            # increment character-level cursor by number of atoms (i.e., characters)
            cursor_idx += sum(len(literal.value) for literal in cur_literals)

        # process final carryover structures
        _, _, _, new_structures, new_hierarchies = self.parse_carryover(
            carryover, {'document': ('end', -1), 'paragraph': ('end', -1)},
            carryover_literals, [],
            carryover_sentences, None,
            cursor_idx
        )
        structures += new_structures
        hierarchies += new_hierarchies

        return cursor_idx, literals, structures, hierarchies
