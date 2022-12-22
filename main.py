from __future__ import annotations

from typing import Union

import pandas as pd

from json import JSONEncoder
import json


# Object Serializer
class Encoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class Lineage:
    tree = dict()

    def __init__(self, designation_date: str, pango: str, partial: str, unaliased: str):
        self.designation_date = designation_date
        self.pango = pango
        self.partial = partial
        self.unaliased = unaliased
        self.parent = Lineage.parent(partial)

    def __repr__(self):
        return f'Lineage(PARENT={self.parent}, DATE={self.designation_date}, PANGO={self.pango}, PARTIAL={self.partial}, UNALIASED={self.unaliased}) '

    @classmethod
    def parent(cls, lineage: str) -> Union[str, None]:
        if "." in lineage:
            return ".".join(lineage.split('.')[:-1])
        return None

    @classmethod
    def add_to_tree(cls, lineage: Lineage):
        if lineage.parent is None:
            # No children to add
            cls.tree.update(
                {
                    'id': lineage.partial,
                    'data': lineage,
                    'parent': None,
                }
            )

        elif 'children' in cls.tree:
            count = len(cls.tree['children'].items())
            cls.tree['children'][count] = {
                'id': lineage.partial,
                'data': lineage,
                'parent': lineage.parent,
            }
        else:
            cls.tree['children'] = {
                0: {
                    'id': lineage.partial,
                    'data': lineage,
                    'parent': lineage.parent,
                }
            }

    @classmethod
    def get_tree(cls):
        return cls.tree


if __name__ == '__main__':
    lineages = pd.read_csv('lineages.tsv', sep='\t', index_col=3)
    lineages['designation_date'] = lineages.index

    for index, (pango, partial, unaliased, issue, date) in lineages.iterrows():
        lineage = Lineage(date, pango, partial, unaliased)
        Lineage.add_to_tree(lineage)

    tree = Lineage.get_tree()
    pretty = json.dumps(tree, indent=4, cls=Encoder)
    print(pretty)
