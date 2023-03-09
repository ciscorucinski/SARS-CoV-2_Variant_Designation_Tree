import pandas as pd
from typing import Union, List
import json


class Lineage:
    tree = dict()
    children_tree = dict()
    parent_tree = dict()
    roots = list()

    def __init__(self, designation_date: str, pango: str, partial: str, unaliased: str):
        self.designation_date = designation_date
        self.pango = pango
        self.partial = partial
        self.unaliased = unaliased
        self.parent = Lineage.get_parent(partial)

    def __repr__(self):
        return f'Lineage(PARENT={self.parent}, DATE={self.designation_date}, PANGO={self.pango}, PARTIAL={self.partial}, UNALIASED={self.unaliased}) '

    @classmethod
    def get_parent(cls, lineage: str) -> Union[str, None]:
        if "." in lineage:
            return ".".join(lineage.split(".")[:-1])
        return None

    @classmethod
    def get_parents_list(cls, lineage: str) -> List[str]:
        parents = list()
        while lineage is not None:
            parents.append(lineage)
            lineage = Lineage.get_parent(lineage)
        return parents

    @classmethod
    def add_to_tree(cls, lineage: "Lineage"):

        id = lineage.partial
        parent = lineage.parent
        parents = Lineage.get_parents_list(parent)

        print(id, parent, parents)
        print()

        assert lineage.designation_date != "#NAME?"
        assert lineage.designation_date != "Loading..."
        assert lineage.designation_date != "#VALUE!"

        node = {
            "id": id,
            "lineage": {
                "pango": lineage.pango,
                "partial": lineage.partial,
                "unaliased": lineage.unaliased,
                "designation_date": lineage.designation_date
            },
            "height": len(parents) if parents else 0,
            "parent": {
                "line_of_descent": parents if parents else None,
                "root": parents[-1] if parents else lineage.partial
            },
            "children": []
        }

        cls.parent_tree[id] = parents

        cls.children_tree[id] = node

        if lineage.parent is None:
            # No children to add
            cls.tree.update(node)
            cls.roots.append(id)
        elif parent in cls.children_tree:
            cls.children_tree[parent]["children"].append(node)
            # node["parent"] = cls.children_tree[parent]
            # node["parent"] = lineage.parent

            # count = len(cls.tree["children"])
            # cls.tree["children"] = {count: {}}
            # cls.tree["children"][count] = node

    @classmethod
    def get_tree(cls, *, roots=None):
        tree: dict = {
            "root": {
                "id": "omicron",
                "lineage": {
                    "pango": "omicron",
                    "partial": "omicron",
                    "unaliased": "omicron",
                    "designation_date": None
                },
                "height": None,
                "parent": {
                    "line_of_descent": None,
                    "root": None
                },
                "children": []
            }
        }

        roots = cls.roots if roots is None else roots  # Default or user-defined root

        if len(roots) == 1:
            return cls.children_tree.get(roots[0])

        for root in roots:
            branch = cls.children_tree.get(root)
            tree["root"]["children"].append(branch)

        return tree

    @classmethod
    def get_progenitors(cls, lineage: str):
        result = []
        parents = cls.get_parents_list(lineage)
        for progenitor in parents:
            parent = cls.get_children().get(progenitor)
            parent["children"] = None
            parent["parent"] = None
            result.append(parent)
        return result

    @classmethod
    def get_parent_id_list(cls):
        return cls.parent_tree

    @classmethod
    def get_children(cls):
        return cls.children_tree

    @classmethod
    def get_roots(cls):
        return cls.roots


if __name__ == "__main__":

    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRcJqvDKlzyT7uNVe4IjuksUoQ3vIIUKJpOnLzYuAxf3cl2Ssp02MedPiBnHUaPfwP24iYSj5a0DHCT/pub?gid=397158123&single=true&output=csv"

    # set index to sort by date. Have to copy date column to still have access to the data
    lineages = pd.read_csv(url, sep=",")

    for index, (pango, partial, unaliased, date, issue, *_) in lineages.iterrows():
        lineage = Lineage(date, pango, partial, unaliased)
        Lineage.add_to_tree(lineage)

    json_object = json.dumps(Lineage.get_tree(),
                             indent=4, sort_keys=False, ensure_ascii=False, separators=(',', ': '),
                             check_circular=False)

    with open("lineages.json", "w") as outfile:
        outfile.write(json_object)
