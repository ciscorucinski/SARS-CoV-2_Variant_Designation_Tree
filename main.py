import pprint
import urllib
from pathlib import Path

import requests

import pandas as pd
from typing import Union, List
import json

import re

from storage import LastUpdatedStorage, LineageTreeStorage, NextCladeLineagesStorage


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


def clean(lineages):
    if isinstance(lineages, str):
        return lineages.replace("*", "")
    else:
        return [lineage.replace("*", "") for lineage in lineages]


def decompress(lineages):
    if '.' not in lineages:
        return lineages
    alias, rest = lineages.split('.', maxsplit=1)
    unaliased_pango_lineage = alias_keys.get(alias, alias)
    return f'{unaliased_pango_lineage}.{rest}'


def contains(lineage, value):
    if 'X' in lineage:
        alias = lineage.split('.', maxsplit=1)[0]
        for lineage in all_alias_keys.get(alias, alias):
            if value in decompress(lineage):
                return True
        return False
    return value in lineage


def extract_pango_values(node):
    # Create an empty list to store PANGO values
    pango_values = []

    # Check if the current node has a "Nextclade_pango" attribute
    if "Nextclade_pango" in node["node_attrs"]:
        # Extract the PANGO value and add it to the list
        pango_values.append(node["node_attrs"]["Nextclade_pango"]["value"])

    # Check if the current node has any children
    if "children" in node:
        # Recursively call the function for each child node and extend the list with the returned values
        for child in node["children"]:
            pango_values.extend(extract_pango_values(child))

    return pango_values


if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', 200)
    # pd.set_option('display.max_colwidth', 200)
    # pd.set_option('display.max_rows', None)

    Path("files/nextclade").mkdir(parents=True, exist_ok=True)
    last_updated_file = LastUpdatedStorage("files/nextclade/last_updated.csv")
    lineage_tree_file = LineageTreeStorage("files/tree.json")
    nextClade_lineages_file = NextCladeLineagesStorage("files/nextclade/available_lineages.txt")

    # url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRcJqvDKlzyT7uNVe4IjuksUoQ3vIIUKJpOnLzYuAxf3cl2Ssp02MedPiBnHUaPfwP24iYSj5a0DHCT/pub?gid=397158123&single=true&output=csv"
    designation_dates_url = "https://raw.githubusercontent.com/ciscorucinski/pango-designation-dates/main/data/lineage_designation_date.csv"
    alias_keys_url = "https://raw.githubusercontent.com/cov-lineages/pango-designation/master/pango_designation/alias_key.json"
    lineage_notes_url = "https://raw.githubusercontent.com/cov-lineages/pango-designation/master/lineage_notes.txt"
    nextclade_mn908947_versions_url = "https://github.com/nextstrain/nextclade_data/tree/master/data/datasets/sars-cov-2/references/MN908947/versions"

    designation_dates_df = pd.read_csv(designation_dates_url, sep=",", parse_dates=['designation_date'], date_format='mixed')

    designation_dates_df.dropna(axis='rows', how='any', subset=['designation_date'], inplace=True)
    designation_dates_df['designation_date'] = pd.to_datetime(designation_dates_df['designation_date'], errors='coerce',
                                                              utc=True)
    designation_dates_df.rename(columns={"lineage": "pango_lineage"}, inplace=True)

    with urllib.request.urlopen(alias_keys_url) as response:
        alias_keys_json = json.loads(response.read().decode())

    all_alias_keys = {key: clean(value) for key, value in alias_keys_json.items() if value != ""}
    alias_keys = {key: clean(value) for key, value in alias_keys_json.items() if value != "" and isinstance(value, str)}
    recombinant_keys = {key: clean(value) for key, value in alias_keys_json.items() if isinstance(value, list)}

    column_order_dataframe = ['alias', 'pango_lineage', 'unaliased_pango_lineage', 'omicron']
    column_order_lineage_notes_dataframe = ['pango_lineage', 'partial_pango_lineage', 'unaliased_pango_lineage',
                                            'designation_date', 'omicron']

    # Alias Keys
    alias_keys_df = pd.DataFrame({k: v for k, v in alias_keys.items() if isinstance(v, str)}.items(),
                                 columns=['alias', 'unaliased_pango_lineage'])

    alias_keys_df['partial_pango_lineage'] = alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineage: lineage.replace('B.1.1.529', 'BA'))
    alias_keys_df['omicron'] = alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineage: contains(lineage, 'B.1.1.529'))

    alias_keys_df = alias_keys_df.reindex(
        columns=['alias', 'partial_pango_lineage', 'unaliased_pango_lineage', 'omicron'])

    # Recombinant Alias Keys
    recombinant_lineages = {k: ", ".join(v) for k, v in recombinant_keys.items() if not isinstance(v, str)}

    recombinant_alias_keys_df = pd.DataFrame([(alias, lineages) for alias, lineages in recombinant_lineages.items()],
                                             columns=['alias', 'pango_lineage'])

    recombinant_alias_keys_df['unaliased_pango_lineage'] = recombinant_alias_keys_df['pango_lineage'].apply(
        lambda lineages: ', '.join([decompress(lineage) for lineage in set(lineages.split(', '))]))

    recombinant_alias_keys_df['omicron'] = recombinant_alias_keys_df['unaliased_pango_lineage'].apply(
        lambda lineages: any([contains(lineage, 'B.1.1.529') for lineage in lineages.split(', ')]))

    recombinant_alias_keys_df = recombinant_alias_keys_df.reindex(columns=column_order_dataframe)

    lineage_notes_df = pd.read_csv(lineage_notes_url, delimiter='\t', on_bad_lines='warn')

    regex = r"^Alias of\s([^\s|,]*)"
    regex_withdrawn = r"^Withdrawn:[\w\s.-]+Alias of\s([^\s|,]*)"
    regex_recombinant = r"^(X[A-Z0-9.]+)"

    lineage_notes_df.rename(columns={'Lineage': 'pango_lineage'}, inplace=True)

    withdrawn_lineage_notes_df = lineage_notes_df.copy()
    recombinant_lineage_notes_df = lineage_notes_df.copy()

    lineage_notes_df['unaliased_pango_lineage'] = lineage_notes_df.apply(
        lambda row: match.group(1) if (match := re.search(regex, row['Description'])) else row[
            'pango_lineage'] if contains(row['pango_lineage'], 'B.1.1.529') else decompress(row['pango_lineage']),
        axis=1
    )

    lineage_notes_df['omicron'] = lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: contains(lineage, 'B.1.1.529'))

    lineage_notes_df['partial_pango_lineage'] = lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: lineage.replace('B.1.1.529.', 'BA.') if lineage else None)

    lineage_notes_df.drop('Description', axis=1, inplace=True)

    recombinant_lineage_notes_df['unaliased_pango_lineage'] = recombinant_lineage_notes_df['pango_lineage'].apply(
        lambda lineage: match.group(1) if (match := re.search(regex_recombinant, lineage)) else None)

    recombinant_lineage_notes_df.dropna(subset=['unaliased_pango_lineage'], inplace=True)
    recombinant_lineage_notes_df.drop('Description', axis=1, inplace=True)

    recombinant_lineage_notes_df['omicron'] = recombinant_lineage_notes_df['pango_lineage'].apply(
        lambda lineage: contains(lineage, 'B.1.1.529'))

    withdrawn_lineage_notes_df['unaliased_pango_lineage'] = withdrawn_lineage_notes_df['Description'].apply(
        lambda description: match.group(1) if (match := re.search(regex_withdrawn, description)) else None)

    withdrawn_lineage_notes_df.dropna(subset=['unaliased_pango_lineage'], inplace=True)
    withdrawn_lineage_notes_df.drop('Description', axis=1, inplace=True)

    withdrawn_lineage_notes_df['partial_pango_lineage'] = withdrawn_lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: lineage.replace('B.1.1.529.', 'BA.') if lineage else None)

    withdrawn_lineage_notes_df['omicron'] = withdrawn_lineage_notes_df['unaliased_pango_lineage'].apply(
        lambda lineage: contains(lineage, 'B.1.1.529'))

    lineage_notes_df = lineage_notes_df.merge(designation_dates_df, on="pango_lineage")
    lineage_notes_df = lineage_notes_df.reindex(columns=column_order_lineage_notes_dataframe)

    for index, (pango, partial, unaliased, date, is_omicron) in lineage_notes_df.iterrows():

        print(f"Analyzing Lineage: {pango} ... ", end='')
        if not is_omicron:
            print("(not processed)")
            continue

        date = date.strftime('%Y-%m-%d %H:%M:%S%z')
        lineage = Lineage(date, pango, partial, unaliased)
        Lineage.add_to_tree(lineage)
        print(f"Added {lineage=}")

    json_object = json.dumps(Lineage.get_tree(),
                             indent=4, sort_keys=False, ensure_ascii=False, separators=(',', ': '),
                             check_circular=False)

    lineage_tree_file.write(json_object)

    print()
    print("Retrieving NextClade Data")

    last_updated, last_retrieved_commit = last_updated_file.read()

    response = requests.get("https://github.com/nextstrain/nextclade_data/blob/master/data/nextstrain/sars-cov-2/MN908947/tree.json")

    payload = json.loads(response.content)['payload']
    repo = payload['repo']
    path = payload['path']
    repository = f"{repo['ownerLogin']}/{repo['name']}"
    branch = repo['defaultBranch']
    latest_commit = payload['refInfo']['currentOid']

    current_latest_tree_url = f"https://raw.githubusercontent.com/{repository}/{branch}/{path}"

    print(f"Latest URL: {current_latest_tree_url}")
    print()

    omicron_pango_values = set()

    if last_retrieved_commit == latest_commit:
        print("No new data is available")
        print(f"Url was already parsed: {last_updated}")
        print()
        print("Retrieving Omicron Lineages ... ", end="")

        omicron_lineages = nextClade_lineages_file.read()

        print("Done")

    else:

        last_updated_file.write(latest_commit)

        print("New data available")
        print("Loading new tree ... ", end="")
        with urllib.request.urlopen(current_latest_tree_url) as response:
            nextstrain_tree = json.loads(response.read().decode())

        print("Done")
        tree = nextstrain_tree["tree"]

        print("Extracting Omicron Lineages ... ", end="")
        all_pango_values = extract_pango_values(tree)
        omicron_lineages = {lineage for lineage in all_pango_values if contains(decompress(lineage), "B.1.1.529")}
        omicron_lineages = sorted(omicron_lineages)

        nextClade_lineages_file.write(omicron_lineages)

        print("Done")

    print()

    # Print the list of PANGO values
    pprint.pprint(omicron_lineages)
