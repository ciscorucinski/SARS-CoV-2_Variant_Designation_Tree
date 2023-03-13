import urllib

import pandas as pd
from typing import Union, List
import json

import re


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
        self.parent = Lineage.get_parent(unaliased)

    def __repr__(self):
        return f'Lineage(PARENT={self.parent}, DATE={self.designation_date}, PANGO={self.pango}, PARTIAL={self.partial}, UNALIASED={self.unaliased}) '

    @classmethod
    def get_parent(cls, lineage: str) -> Union[str, None]:
        if "." in lineage and not lineage == 'B.1.1.529':
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

        print(f"Adding {lineage=}")

        id = lineage.unaliased
        parent = lineage.parent
        parents = Lineage.get_parents_list(parent)

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

        if lineage.parent is None or lineage.pango == 'B.1.1.529':
            # No children to add
            cls.tree.update(node)
            cls.roots.append(id)
        elif parent in cls.children_tree or parent == 'BA':
            cls.children_tree[parent]["children"].append(node)
            print(cls.children_tree[parent]["children"])

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


def clean(lineages):
    return [lineage.replace("*", "") for lineage in lineages]


def decompress(lineages):
    partial_pango = []

    for lineage in lineages:
        print(lineage)
        alias, *rest = lineage.split('.', maxsplit=1)
        unaliased_pango_lineage = all_alias_keys.get(alias, alias)
        if isinstance(unaliased_pango_lineage, list):
            partial_pango.extend(decompress(unaliased_pango_lineage))
        else:
            partial_pango.append(f'{unaliased_pango_lineage}.{rest}')

    return partial_pango


if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', 200)
    # pd.set_option('display.max_colwidth', 200)
    # pd.set_option('display.max_rows', None)

    # url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRcJqvDKlzyT7uNVe4IjuksUoQ3vIIUKJpOnLzYuAxf3cl2Ssp02MedPiBnHUaPfwP24iYSj5a0DHCT/pub?gid=397158123&single=true&output=csv"
    designation_dates_url = "https://raw.githubusercontent.com/ciscorucinski/pango-designation-dates/main/data/lineage_designation_date.csv"
    alias_keys_url = "https://raw.githubusercontent.com/cov-lineages/pango-designation/master/pango_designation/alias_key.json"
    lineage_notes_url = "https://raw.githubusercontent.com/cov-lineages/pango-designation/master/lineage_notes.txt"

    # set index to sort by date. Have to copy date column to still have access to the data
    # lineages_df = pd.read_csv(url, sep=",")

    designation_dates_df = pd.read_csv(designation_dates_url, sep=",", parse_dates=['designation_date'])

    designation_dates_df.dropna(axis='rows', how='any', subset=['designation_date'], inplace=True)
    designation_dates_df['designation_date'] = pd.to_datetime(designation_dates_df['designation_date'], errors='coerce',
                                                              utc=True)
    designation_dates_df.rename(columns={"lineage": "pango_lineage"}, inplace=True)

    with urllib.request.urlopen(alias_keys_url) as response:
        alias_keys_json = json.loads(response.read().decode())

    all_alias_keys = {key: value for key, value in alias_keys_json.items() if value != ""}
    alias_keys = {key: value for key, value in alias_keys_json.items() if value != "" and isinstance(value, str)}
    recombinant_keys = {key: value for key, value in alias_keys_json.items() if isinstance(value, list)}

    column_order_dataframe = ['alias', 'pango_lineage', 'unaliased_pango_lineage_lineage', 'omicron']
    column_order_lineage_notes_dataframe = ['pango_lineage', 'partial_pango_lineage', 'unaliased_pango_lineage',
                                            'designation_date', 'omicron']

    # Alias Keys
    alias_keys_df = pd.DataFrame({k: v for k, v in alias_keys.items() if isinstance(v, str)}.items(),
                                 columns=['alias', 'unaliased_pango_lineage'])

    alias_keys_df['partial_pango_lineage'] = alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineage: lineage.replace('B.1.1.529', 'BA'))
    alias_keys_df['omicron'] = alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineage: True if lineage.startswith('B.1.1.529') else False)

    alias_keys_df = alias_keys_df.reindex(
        columns=['alias', 'partial_pango_lineage', 'unaliased_pango_lineage', 'omicron'])

    # Recombinant Alias Keys
    recombinant_lineages = {k: ", ".join(clean(v)) for k, v in recombinant_keys.items() if not isinstance(v, str)}

    recombinant_alias_keys_df = pd.DataFrame([(alias, lineages) for alias, lineages in recombinant_lineages.items()],
                                             columns=['alias', 'pango_lineage'])

    recombinant_alias_keys_df['unaliased_pango_lineage'] = recombinant_alias_keys_df['pango_lineage'].apply(
        lambda lineage: ", ".join(decompress(lineage.split(', '))))
    recombinant_alias_keys_df['omicron'] = recombinant_alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineage: True if 'B.1.1.529' in lineage else False)

    recombinant_alias_keys_df = recombinant_alias_keys_df.reindex(columns=column_order_dataframe)

    lineage_notes_df = pd.read_csv(lineage_notes_url, delimiter='\t')

    regex = r"^Alias of\s([^\s|,]*)"
    regex_withdrawn = r"^Withdrawn:[\w\s.-]+Alias of\s([^\s|,]*)"
    regex_recombinant = r"^(X[A-Z0-9.]+)"

    lineage_notes_df.rename(columns={'Lineage': 'pango_lineage'}, inplace=True)

    withdrawn_lineage_notes_df = lineage_notes_df.copy()
    recombinant_lineage_notes_df = lineage_notes_df.copy()

    lineage_notes_df['unaliased_pango_lineage'] = lineage_notes_df.apply(
        lambda row: match.group(1) if (match := re.search(regex, row['Description'])) else row['pango_lineage'],
        axis=1
    )

    lineage_notes_df['omicron'] = lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: 'B.1.1.529' in (
            lineage if lineage[0] != 'X' else ", ".join(decompress(lineage.split(', ')))) if lineage else False)

    lineage_notes_df['partial_pango_lineage'] = lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: lineage.replace('B.1.1.529.', 'BA.') if lineage else None)

    # lineage_notes_df.dropna(subset=['unaliased_pango_lineage'], inplace=True)
    lineage_notes_df.drop('Description', axis=1, inplace=True)

    print(lineage_notes_df)

    recombinant_lineage_notes_df['unaliased_pango_lineage'] = recombinant_lineage_notes_df['pango_lineage'].apply(
        lambda lineage: match.group(1) if (match := re.search(regex_recombinant, lineage)) else None)

    recombinant_lineage_notes_df.dropna(subset=['unaliased_pango_lineage'], inplace=True)
    recombinant_lineage_notes_df.drop('Description', axis=1, inplace=True)
    recombinant_lineage_notes_df.drop(index=recombinant_lineage_notes_df.index[:3], inplace=True)

    recombinant_lineage_notes_df['omicron'] = recombinant_lineage_notes_df['pango_lineage'].apply(
        lambda lineage: True)  # TODO This needs to be way more robust

    withdrawn_lineage_notes_df['unaliased_pango_lineage'] = withdrawn_lineage_notes_df['Description'].apply(
        lambda description: match.group(1) if (match := re.search(regex_withdrawn, description)) else None)

    withdrawn_lineage_notes_df['pango_lineage'] = withdrawn_lineage_notes_df['pango_lineage'].apply(
        lambda lineage: lineage[1:])

    withdrawn_lineage_notes_df.dropna(subset=['unaliased_pango_lineage'], inplace=True)
    withdrawn_lineage_notes_df.drop('Description', axis=1, inplace=True)

    withdrawn_lineage_notes_df['omicron'] = withdrawn_lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: True if ", ".join(decompress(lineage.split(', '))).startswith('B.1.1.529') else False)

    lineage_notes_df = lineage_notes_df.merge(designation_dates_df, on="pango_lineage")
    lineage_notes_df = lineage_notes_df.reindex(columns=column_order_lineage_notes_dataframe)

    print(lineage_notes_df)

    for index, (pango, partial, unaliased, date, is_omicron) in lineage_notes_df.iterrows():
        if not is_omicron:
            continue
        date = date.strftime('%Y-%m-%d %H:%M:%S%z')
        lineage = Lineage(date, pango, partial, unaliased)
        Lineage.add_to_tree(lineage)

    json_object = json.dumps(Lineage.get_tree(),
                             indent=4, sort_keys=False, ensure_ascii=False, separators=(',', ': '),
                             check_circular=False)

    with open("lineages.json", "w") as outfile:
        outfile.write(json_object)
