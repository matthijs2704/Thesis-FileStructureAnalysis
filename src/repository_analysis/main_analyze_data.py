from pathlib import Path
import time
from data_analysis import extract_commits_of_interst
import pandas as pd
import argparse
import json
from data_analysis.file_tree_evolution import analyze_tree_evolution
from treelib import Node, Tree
import itertools
from collections import defaultdict
import plotly.graph_objects as go
import plotly.express as px
from pprint import pprint
import yaml
import pickle

parser = argparse.ArgumentParser()
parser.add_argument("project_path", type=Path, help="project path for data files")

args = parser.parse_args()
project_path: Path = args.project_path
project_data_file = project_path / "data.jsonl"
project_commits_file = project_path / "commits.jsonl"
project_repo_info_file = project_path / "repo_info.json"

linguist_langs = yaml.safe_load(
    open("commit_analysis/utils/datasets/linguist/languages.yml", "r")
)
linguist_color_map = {k: v.get("color", "#000000") for k, v in linguist_langs.items()}
linguist_color_map["Unknown"] = "#000000"
linguist_color_map["None"] = "#000000"
# def file_tree_to_language_tree(tree: Tree) -> Tree:
#     dir_file_languages = {}
#     for dirNodeId in tree.expand_tree(filter=lambda x: not x.is_leaf()):
#         dir: Node = tree[dirNodeId]

#         files_in_dir = list(
#             filter(lambda x: x.is_leaf(), tree.children(dir.identifier))
#         )

#         for subpath in dir.identifier.split("/"):
#             if subpath not in dir_file_languages:
#                 dir_file_languages[subpath] = {}

#         dir_file_languages[dir.identifier] = {
#             k: len(list(v))
#             for k, v in itertools.groupby(
#                 list(map(lambda f: f.data.language, files_in_dir))
#             )
#         }
#     return dir_file_languages


def file_tree_to_language_tree(tree: Tree) -> Tree:
    dir_file_languages = Tree()
    dir_file_languages.create_node("root", "root")  # Create root node
    dir_file_languages["root"].data = tree[tree.root].data

    for dirNodeId in tree.expand_tree(filter=lambda x: not x.is_leaf()):
        dir: Node = tree[dirNodeId]

        files_in_dir = list(
            filter(lambda x: x.is_leaf(), tree.children(dir.identifier))
        )

        parent_id = "root"
        for subpath in dir.identifier.split("/"):
            node_id = f"{parent_id}/{subpath}" if parent_id != "root" else subpath
            if not dir_file_languages.contains(node_id):
                dir_file_languages.create_node(subpath, node_id, parent=parent_id)
            parent_id = node_id

        dir_file_languages[node_id].data = {
            k: len(list(v))
            for k, v in itertools.groupby(
                list(map(lambda f: f.data.language, files_in_dir))
            )
        }
    return dir_file_languages


# def file_tree_to_language_tree_dict(tree: Tree, node_id) -> dict:
#     node = tree[node_id]
#     children = tree.children(node_id)
#     files_in_dir = list(filter(lambda x: x.is_leaf(), children))
#     file_languages = {
#         k: len(list(v))
#         for k, v in itertools.groupby(map(lambda f: f.data.language, files_in_dir))
#     }
#     node_dict = {node.tag: file_languages}
#     for child in children:
#         if not child.is_leaf():
#             node_dict[node.tag].update(
#                 file_tree_to_language_tree_dict(tree, child.identifier)
#             )
#     return node_dict


# def tree_to_dict(tree, node_id):
#     node = tree[node_id]
#     children = tree.children(node_id)
#     if children:
#         return {child.tag: tree_to_dict(tree, child.identifier) for child in children}
#     else:
#         return None


if __name__ == "__main__":
    df_mutations = pd.read_json(project_data_file, lines=True)
    df_commits = pd.read_json(project_commits_file, lines=True).set_index("commit_id")
    with open(project_repo_info_file, "r") as f:
        repo_info = json.load(f)

    df = df_mutations.merge(df_commits, on="commit_id")
    df = df.sort_values(by="commit_time", ascending=True)

    # Identify commits of interest
    threshold = 0.8
    coi = extract_commits_of_interst(df_commits, threshold)
    print(f"Found {len(coi)} commits of interest")
    coi.to_json(project_path / "coi.jsonl", orient="records", lines=True)

    # Analyse tree evolution
    if (project_path / "tree_evolution.pkl").exists():
        with open(project_path / "tree_evolution.pkl", "rb") as f:
            tree_evolution = pickle.load(f)
    else:
        start_time = time.time()
        every_10th_commit_id = df_commits.index[::100]
        print(f"Analyzing {len(every_10th_commit_id)} commits")
        tree_evolution = analyze_tree_evolution(
            df_commits, df_mutations, every_10th_commit_id
        )
        with open(project_path / "tree_evolution.pkl", "wb") as f:
            pickle.dump(tree_evolution, f)
        all_trees_build = time.time() - start_time
        print(f"Tree evolution took {all_trees_build} seconds")

    start_time = time.time()

    # last_tree = tree_evolution[df_commits.index[-1]]
    lang_trees = {k: file_tree_to_language_tree(v) for k, v in tree_evolution.items()}
    lang_tree = list(lang_trees.values())[-1]
    # lang_tree = file_tree_to_language_tree(last_tree)
    print(f"Lang Tree evolution took {time.time() - start_time} seconds")

    def tree_to_lang_overview(tree: Tree):
        lang_overview = defaultdict(int)
        for node_id in tree.expand_tree(mode=Tree.WIDTH):
            node = tree[node_id]

            for lang, count in node.data.items():
                lang_overview[lang or "Unknown"] += count
        # Lang overview to percentage
        total = sum(lang_overview.values())
        for lang, count in lang_overview.items():
            lang_overview[lang] = count
        return dict(lang_overview)

    lang_overviews = {k: tree_to_lang_overview(v) for k, v in lang_trees.items()}

    from pprint import pprint

    pprint(lang_overviews)
    import plotly.graph_objects as go

    # Assuming lang_overviews is a list of dictionaries
    # where each dictionary represents a language distribution at a certain point in time

    # Get all unique languages
    languages = set().union(*lang_overviews.values())

    # Create a trace for each language
    traces = []
    for lang in languages:
        percentages = [d.get(lang, 0) for d in lang_overviews.values()]
        traces.append(
            go.Scatter(
                x=list(lang_overviews.keys()),
                y=percentages,
                mode="lines",
                name=lang,
            )
        )

    # Create the line chart
    fig = go.Figure(data=traces)

    # Customize aspect
    fig.update_layout(
        title_text="Language Overview Over Time",
        xaxis_title="Time",
        yaxis_title="Percentage",
    )

    fig.show()
    fig.update_layout(height=800, width=1024)  # Set the width to 800 pixels
    fig.write_image("file_types_over_time.pdf")
    # exit(1)

    file_type_overview = defaultdict(int)
    file_types_per_level = defaultdict(lambda: defaultdict(int))
    for node_id in lang_tree.expand_tree(mode=Tree.WIDTH):
        node = lang_tree[node_id]
        depth = len(node.identifier.split("/")) - 1
        for lang, count in node.data.items():
            file_type_overview[lang or "Unknown"] += count
            file_types_per_level[depth][lang] += count

    print("File type overview")
    print(file_type_overview)

    print("File types per level")
    print(file_types_per_level)

    # # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame(list(file_type_overview.items()), columns=["FileType", "Count"])
    print(df.describe())
    # Create the treemap
    fig = px.sunburst(df, path=["FileType"], values="Count")
    fig.show()
    fig.update_layout(height=500, width=500)
    fig.write_image("file_types_dist.pdf")

    # # Convert the defaultdict to a regular dict for compatibility with pandas
    file_types_per_level_dict = {k: dict(v) for k, v in file_types_per_level.items()}

    # Flatten the dictionary and convert it to a pandas DataFrame
    df = pd.DataFrame(
        [
            (level, filetype or "Unknown", count)
            for level, files in file_types_per_level_dict.items()
            for filetype, count in files.items()
        ],
        columns=["Level", "FileType", "Count"],
    )

    # Create the treemap
    fig = px.sunburst(df, path=["Level", "FileType"], values="Count")
    fig.show()
    fig.update_layout(height=500, width=500)
    fig.write_image("files_per_level.pdf")

    dir_color = "#636efa"

    def dict_to_lists(tree: Tree):
        labels = []
        parents = []
        ids = []
        values = []
        colors = []  # New list for colors
        for nid in tree.expand_tree(mode=Tree.WIDTH):
            n = tree[nid]
            labels.append(n.tag)
            parents.append(
                tree.parent(n.identifier).identifier if n.identifier != "root" else ""
            )
            ids.append(n.identifier)

            if n.data and len(n.data) > 0:
                values.append(len(tree.children(nid)) + sum(n.data.values()))
                colors.append(dir_color)
                # Add each language as a separate node
                for lang, count in n.data.items():
                    labels.append(lang or "Unknown")
                    parents.append(n.identifier)
                    ids.append(f"{n.identifier}/{lang}")
                    values.append(count)
                    colors.append(
                        linguist_color_map.get(lang, "yellow")
                        if lang is not None
                        else "black"
                    )
            else:
                values.append(len(tree.children(nid)))
                colors.append(dir_color)

        return labels, parents, ids, values, colors

    labels, parents, ids, values, colors = dict_to_lists(lang_tree)

    fig = go.Figure(
        go.Sunburst(
            labels=labels,
            parents=parents,
            ids=ids,
            values=values,
            marker=dict(colors=colors),
        )
    )

    fig.show()
    fig.update_layout(height=500, width=500)
    fig.write_image("filetree.pdf")

    # print(dir_file_languages)
