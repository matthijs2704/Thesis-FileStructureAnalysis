import datetime
import fuse
import os
from fuse import FUSE

fuse.fuse_python_api = (0, 1)

from repofs.repofs import RepoFS

import pygit2

import pygit2

repo_url = "https://github.com/ishepard/pydriller.git"
local_path = os.path.abspath("./.repo/repository/")
mount_path = os.path.abspath("./.repo/mount/")
# local_path = os.path.abspath("./pydriller/")

print ("Cloning repo")
pygit2.clone_repository(repo_url, local_path)
print ("Done cloning")
print("Examining repository.  Please wait..\n")
start = datetime.datetime.now()
repo = RepoFS(
    repo=local_path,
    mount=mount_path,
    hash_trees=False,
    no_ref_symlinks=False,
    no_cache=False
)
end = datetime.datetime.now()
print("Ready! Repository mounted in %s\n" % (end - start))
print("Repository %s is now visible at %s\n" % (local_path, repo.mount))
FUSE(repo, os.path.abspath(repo.mount), nothreads=True, foreground=False)