input_file = "/Volumes/SD/PyCharmProjects/cc_coursework/data/soc-Epinions1.txt"

nodes = []
with open(input_file) as f:
    for line in f.readlines():
        line = line.split("\t")
        nodes.append(int(line[0]))
        nodes.append(int(line[1]))

nodes = set(nodes)

min_node_id = min(nodes)
max_node_id = max(nodes)
num_nodes = len(nodes)

print("Min node id: {}, Max node id: {}, Num of unique nodes {}".format(min_node_id, max_node_id, num_nodes))