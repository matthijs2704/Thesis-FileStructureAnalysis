#!/usr/bin/env /bin/bash

# Pass the number of memory blocks (512 B on macOS)
#     2.000 ~ 1 MB usable
#   500.000 ~ 250 MB usable
# 2.000.000 ~ 1 GB usable

if [[ $1 == *G ]]; then
    GBS=$(echo "$1" | sed 's/G//')
    BLOCKS=$(echo "($GBS * 2^21)" | bc)
else
    BLOCKS=$1
fi

diskutil erasevolume HFS+ 'RAMDisk' `hdiutil attach -nobrowse -nomount ram://$BLOCKS`