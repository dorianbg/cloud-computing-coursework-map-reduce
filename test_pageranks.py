"""
the script computes page ranks using the networkx library
"""
import networkx as nx
import heapq
# Initialize directed graph
G = nx.DiGraph()

input_file = "data/soc-Epinions1.txt"
with open(input_file) as f:
    for line in f.readlines():
        line = line.split("\t")
        node1 = int(line[0])
        node2 = int(line[1])
        G.add_edge(node1, node2)

# Compute pagerank (keys are node IDs, values are pageranks)
res = []
pr = nx.pagerank(G, alpha=0.85, max_iter=50)

for key in pr.keys():
    res.append((key, pr[key]))

topn = heapq.nlargest(n=100, iterable=res, key=lambda x: x[1])

for i in topn:
    print(i)
