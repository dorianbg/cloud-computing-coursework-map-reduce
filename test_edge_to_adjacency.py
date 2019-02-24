import json
import os
from pathlib import Path
#folder = "epinions_pagerank_simple"
folder = "test_adjacency_edge"
files = os.listdir(folder)

sum = 0
for file in files:
    if not file.startswith("."):
        path = Path(folder, file)
        print(path)
        with open(path) as f:
            for line in f.readlines():
                value = line.strip("\n").split("\t")[1]
                sum += json.loads(value)['page_rank']
print(sum)
