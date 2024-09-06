from treelib import Node, Tree
from typing import List, Optional, Dict
import pandas as pd


class FileNode(object):
    def __init__(self, language):
        self.language = language


def ensure_parent_exists(tree: Tree, path):
    parent = "root"
    dir_path = []
    for part in path:
        dir_path.append(part)
        cur_path = "/".join(dir_path)
        if tree.contains("/".join(dir_path)):
            parent = cur_path
        else:
            # add the directory as a node in the tree
            tree.create_node(part, cur_path, parent=parent, data=FileNode("DIR"))
            parent = cur_path
    return parent


def add_file_to_tree(tree: Tree, file_path: str, language: str):
    # split the path into individual directories and file name
    path_parts = file_path.split("/")
    file_name = path_parts.pop()

    parentId = ensure_parent_exists(tree, path_parts)
    parent: Node = tree[parentId]
    # add the file as a node in the tree
    if not tree.contains(file_path):
        return tree.create_node(
            file_name, file_path, parent=parent, data=FileNode(language)
        )
    else:
        print("File already exists in tree: " + file_path)
        raise Exception("File already exists in tree: " + file_path)
        return tree[file_path]


def remove_file_from_tree(tree: Tree, file_path: str):
    if not tree.contains(file_path):
        print("Could not remove file, not found in tree: " + file_path)
        raise Exception("Could not remove file, not found in tree: " + file_path)
        return

    node: Node = tree.get_node(file_path)
    parents = list(tree.rsearch(node.identifier))[1:]

    # remove the file from the tree
    tree.remove_node(node.identifier)

    # remove the node parent if it is empty, recursively up the tree
    for parentId in parents:
        if len(tree.children(parentId)) == 0:
            tree.remove_node(parentId)


def move_file_in_tree(tree: Tree, old_path, new_path, new_lang: str):
    if not tree.contains(old_path):
        print("Old file not found in tree: " + old_path)
        # raise Exception("Old file not found in tree: " + old_path)
        return

    if tree.contains(new_path):
        # Assume file has already been moved
        print("File has already been moved: " + new_path)
        # raise Exception("File has already been moved: " + new_path)
        return

    remove_file_from_tree(tree, old_path)
    return add_file_to_tree(tree, new_path, new_lang)


def rename_file_in_tree(tree: Tree, old_path, new_path, new_lang):
    if not tree.contains(old_path):
        print("Could not rename! " + old_path)
        raise Exception("Could not rename! " + old_path)
        return

    remove_file_from_tree(tree, old_path)
    return add_file_to_tree(tree, new_path, new_lang)


def apply_mutations(tree, mutations):
    # perform addition mutations on the tree
    # First perform moves and renames, then additions, deletions
    for _, mutation in mutations[mutations["action"] == "delete"].iterrows():
        remove_file_from_tree(tree, mutation["old_path"])
    for _, mutation in mutations[mutations["action"] == "rename"].iterrows():
        rename_file_in_tree(
            tree, mutation["old_path"], mutation["new_path"], mutation["language"]
        )
    for _, mutation in mutations[mutations["action"] == "move"].iterrows():
        move_file_in_tree(
            tree, mutation["old_path"], mutation["new_path"], mutation["language"]
        )
    for _, mutation in mutations[mutations["action"] == "add"].iterrows():
        add_file_to_tree(tree, mutation["new_path"], mutation["language"])

    return tree


def analyze_tree_evolution(
    commits_df: pd.DataFrame,
    mutation_df: pd.DataFrame,
    keep_tree_for_commits: Optional[List[str]] = None,
) -> Dict[str, Tree]:
    basetree = Tree()
    basetree.create_node("root", "root", data=FileNode("DIR"))

    evolution_hist = {}
    for commit_id, commit_data in commits_df.iterrows():
        # print("Processing commit: " + str(commit_id))
        # custom_action_order = ["delete", "move", "rename", "add"]
        mutations = mutation_df[mutation_df["commit_id"] == commit_id]
        basetree = apply_mutations(basetree, mutations)

        if keep_tree_for_commits is None or commit_id in keep_tree_for_commits:
            evolution_hist[commit_id] = Tree(basetree, deep=False)

    return evolution_hist
