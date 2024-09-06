import json
from pydriller import Repository
from pydriller.domain.commit import ModifiedFile, Commit
from pprint import pprint

from pydriller.domain.commit import ModificationType
from typing import Dict, List
from multiprocessing.pool import Pool

allCommits = []
commitsWithStructureChanges: Dict[str, Dict[str, List[str] | List[Dict[str,str]]]] = []

def is_rename(old_path: str, new_path: str):
    old_path = old_path.split('/')
    new_path = new_path.split('/')
    old_file_name = old_path.pop()
    new_file_name = new_path.pop()
    # if the file name is the same, it's not a rename, but a move
    return '/'.join(old_path) == '/'.join(new_path) and old_file_name != new_file_name

def process(commit: Commit):
    file_mutations = []
    for file in commit.modified_files:
        if (file.change_type == ModificationType.ADD):
            file_mutations.append({"action": "add", "old_path": None, "new_path": file.new_path})

        if (file.change_type == ModificationType.DELETE):
            file_mutations.append({"action": "delete", "old_path": file.old_path, "new_path": None})

        if (file.change_type == ModificationType.RENAME and is_rename(file.old_path, file.new_path)):
            if (file.old_path != file.new_path):
                file_mutations.append({"action": "rename", "old_path": file.old_path, "new_path": file.new_path})

        if (file.change_type == ModificationType.RENAME and not is_rename(file.old_path, file.new_path)):
            if (file.old_path != file.new_path):
                file_mutations.append({"action": "move", "old_path": file.old_path, "new_path": file.new_path})

    if len(file_mutations) > 0:
        return { 'id': commit.hash, 'author': f"{commit.author.name} ({commit.author.email})", 'author_date': commit.author_date.isoformat, 'msg': commit.msg, 'file_mutations': file_mutations }
    else:
        return None
    # print ({ 'id': commit.hash, 'file_mutations': file_mutations })

import threading
csv_writer_lock = threading.Lock()
repo = Repository("/Volumes/RAMDisk/react.git", num_workers=16)

if __name__ == '__main__':
    # with Pool(processes=16) as pool:        
    #     for result in pool.imap_unordered(process, repo.traverse_commits()):
    with open('newout.jsonl', 'a') as f:
        for commit in repo.traverse_commits():
            result = process(commit)
            if result is not None:
                f.write(f'{result}\n')

# END: 9j3k4n5m6p7q
# print(repo._conf.build_args())
exit(0)
def check_if_rename(mf: ModifiedFile):
    if mf.change_type != ModificationType.RENAME:
        return False

    old_path = mf.old_path.split('/')
    new_path = mf.new_path.split('/')

    old_file_name = old_path.pop()
    new_file_name = new_path.pop()

    # if the file name is the same, it's not a rename, but a move
    return '/'.join(old_path) == '/'.join(new_path) and old_file_name != new_file_name


num_commits = 0
for commit in repo.traverse_commits():
    num_commits += 1

    # print(commit.hash)
    # print(commit.msg)
    # print(commit.author.name)
    file_changes = {
        ModificationType.ADD: [],
        ModificationType.RENAME: [],
        ModificationType.COPY: [],
        ModificationType.DELETE: [],
        ModificationType.UNKNOWN: [],
        ModificationType.MODIFY: []
    }

    for file in commit.modified_files:
        file_changes[file.change_type].append(file)
        # if file.change_type == ModificationType.RENAME and check_if_rename(file):
        #     file_changes[ModificationType.RENAME].append(file)

    file_additions = file_changes[ModificationType.ADD]
    file_renames = file_changes[ModificationType.RENAME]
    file_moves = [f for f in file_changes[ModificationType.RENAME] if not check_if_rename(f)]
    file_copies = file_changes[ModificationType.COPY]
    file_deletions = file_changes[ModificationType.DELETE]
    file_unknowns = file_changes[ModificationType.UNKNOWN]
    file_modifications = file_changes[ModificationType.MODIFY]

    if len(file_additions) > 0 or len(file_moves) > 0 or len(file_renames) > 0 or len(file_deletions) > 0 or len(file_copies)>0:
        commitsWithStructureChanges.append(
            {
                commit.hash: {
                    "file_additions": [file.new_path for file in file_additions],
                    "file_moves": [{ 'old': file.old_path, 'new': file.new_path} for file in file_moves],
                    "file_renames": [{ 'old': file.old_path, 'new': file.new_path} for file in file_renames],
                    "file_copies": [{ 'old': file.old_path, 'new': file.new_path} for file in file_copies],
                    "file_deletions": [file.old_path for file in file_deletions],
                    "file_unknowns": [{ 'old': file.old_path, 'new': file.new_path} for file in file_unknowns],

                }
            }
        )
    # for file in commit.modified_files:
    #     print(file.filename, ' has changed')
json_object = json.dumps(commitsWithStructureChanges, indent=2)
with open("data.json", "w") as outfile:
    outfile.write(json_object)

print (f'Commits with structure changes: {len(commitsWithStructureChanges)} / {num_commits}') 

# pprint(commitsWithStructureChanges)
