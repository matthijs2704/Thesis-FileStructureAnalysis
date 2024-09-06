import numpy
import pandas as pd
from treelib import Node, Tree
import uuid

import pandas as pd
import time
import argparse
from pathlib import Path

args = argparse.ArgumentParser(description="Process input project folder.")
args.add_argument("input_datafolder", type=Path, help="input data folder")
args = args.parse_args()
input_datafolder: Path = args.input_datafolder
data_path = input_datafolder / "data.jsonl"
commits_path = input_datafolder / "commits.jsonl"

# read the JSONL file into a pandas DataFrame
df = pd.read_json(data_path, lines=True)
df_commits = pd.read_json(commits_path, lines=True)

df = df.join(df_commits.set_index("commit_id"), on="commit_id")

commits = df["commit_id"].unique().tolist()
# print(df.to_string())
print(str(len(commits)) + " commit(s) with structural changes")

# create an empty tree
tree = Tree()


def ensure_parent_exists(tree: Tree, file_path, is_diff_tree=False):
    path_parts = file_path.split("/")
    parent = "root"
    dir_path = []
    for part in path_parts[:-1]:
        dir_path.append(part)
        cur_path = "/".join(dir_path)
        if tree.contains("/".join(dir_path)):
            parent = cur_path
        else:
            # add the directory as a node in the tree
            tree.create_node(
                part if not is_diff_tree else (part + " (+)"), cur_path, parent=parent
            )
            parent = cur_path
    return parent


def add_file_to_tree(tree: Tree, file_path, is_diff_tree=False):
    # split the path into individual directories and file name
    path_parts = file_path.split("/")
    file_name = path_parts.pop()
    # print ("Adding file " + file_path)

    parent = ensure_parent_exists(tree, file_path, is_diff_tree)

    # add the file as a node in the tree
    if not tree.contains(file_path):
        return tree.create_node(
            file_name if not is_diff_tree else (file_name + " (+)"),
            file_path,
            parent=parent,
        )
    else:
        if is_diff_tree:
            tree[file_path].tag = tree[file_path].tag + " (+)"
            return tree[file_path]
        else:
            print("File already exists in tree: " + file_path)
        # return tree[file_path]


def remove_file_from_tree(tree: Tree, file_path, is_diff_tree=False):
    if not tree.contains(file_path):
        print("Could not remove file, not found in tree: " + file_path)
        return

    # if diff tree, don't remove the file, just mark it as deleted
    if is_diff_tree:
        tree[file_path].tag = tree[file_path].tag + " (–)"
        return

    # print ("Deleting file " + file_path)

    # remove the file from the tree
    tree.remove_node(file_path)

    # remove the parent directories if they are empty
    path_parts = file_path.split("/")[:-1]
    dir_path = []
    for part in path_parts:
        dir_path.append(part)
        cur_path = "/".join(dir_path)
        if not tree.children(cur_path):
            tree.remove_node(cur_path)


def move_file_in_tree(tree: Tree, old_path, new_path, is_diff_tree=False):
    if not tree.contains(old_path):
        print("Old file not found in tree: " + old_path)
        return

    if tree.contains(new_path):
        # Assume file has already been moved
        print("File has already been moved: " + new_path)
        return

    new_filename = new_path.split("/")[-1]

    parent = ensure_parent_exists(tree, new_path)
    tree.remove_node(old_path)

    if is_diff_tree:
        # tree[old_path].tag = f"{old_filename} (-> {new_path}))"
        if tree.contains(new_path):
            tree[new_path].tag = tree[new_path].tag + f" (<- {old_path})"
            return tree[new_path]
        return tree.create_node(
            new_filename + f" (<- {old_path})", new_path, parent=parent
        )

    return tree.create_node(new_filename, new_path, parent=parent)


def rename_file_in_tree(tree: Tree, old_path, new_path, is_diff_tree=False):
    old_filename = (
        tree[old_path].tag if tree.contains(old_path) else old_path.split("/")[-1]
    )
    new_filename = new_path.split("/")[-1]

    if not tree.contains(old_path):
        print("Could not rename! " + old_path)
        return
        # tree.remove_node(old_path)

    parent = ensure_parent_exists(tree, new_path, is_diff_tree)
    tree.remove_node(old_path)

    if is_diff_tree:
        if tree.contains(new_path):
            tree[new_path].tag = tree[new_path].tag + f" (<- {old_filename})"
            return tree[new_path]
        return tree.create_node(
            new_filename + f" (<- {old_filename})", new_path, parent=parent
        )

    tree.create_node(new_filename, new_path, parent=parent)


# iterate over each row in the dataframe
def apply_mutations(tree, mutations, is_diff_tree=False):
    # perform addition mutations on the tree
    for _, mutation in mutations.iterrows():
        if mutation["action"] == "add":
            add_file_to_tree(tree, mutation["new_path"], is_diff_tree)
        elif mutation["action"] == "delete":
            remove_file_from_tree(tree, mutation["old_path"], is_diff_tree)
        elif mutation["action"] == "rename":
            rename_file_in_tree(
                tree, mutation["old_path"], mutation["new_path"], is_diff_tree
            )
        elif mutation["action"] == "move":
            move_file_in_tree(
                tree, mutation["old_path"], mutation["new_path"], is_diff_tree
            )
    return tree


basetree = Tree()
basetree.create_node("root", "root")

combine_n_commits = 1

import os

# specify the directory path
dir_path = "pic"

# iterate over the files in the directory and delete them
# for file_name in os.listdir(dir_path):
#     file_path = os.path.join(dir_path, file_name)
#     if os.path.isfile(file_path):
#         os.remove(file_path)
num_files_per_commit = {}
# for i in range(0, 5*combine_n_commits, combine_n_commits):
for i in range(0, len(commits), combine_n_commits):
    commit_ids = commits[i : i + combine_n_commits]
    muts = df[df["commit_id"].isin(commit_ids)].sort_values(
        by=["commit_time", "action"], ascending=True
    )
    # print(muts)
    # muts.to_csv(f'debug/muts_{i}_{i+combine_n_commits}.csv')
    # exit(1)
    # difftree = Tree(basetree.subtree(basetree.root), deep=True)

    # apply_mutations(difftree, muts, is_diff_tree=True)
    apply_mutations(basetree, muts)

    # difftree.save2file(f"{dir_path}/tree_{i}.txt", sorting=True)

    # number of files in each level
    items_in_level = {}
    node: Node
    for node in basetree.all_nodes_itr():
        lvl = basetree.level(node.identifier)
        items_in_level[lvl] = items_in_level.get(lvl, 0) + 1

    num_files_per_commit[i] = items_in_level
    # print(difftree.all_nodes_itr())
print(num_files_per_commit)
