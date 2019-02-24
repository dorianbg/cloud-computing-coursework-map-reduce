from mrjob.job import MRJob, MRStep
from mrjob.protocol import TextProtocol
from collections import defaultdict


class MRPageRank(MRJob):
    INPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg(
            '--n_nodes', type=int, default=75879, help='Number of nodes in the graph')

    def convert_edge_list_to_adjacency_list_reducer1(self, node_id, nodes):
        """
        converts the edge list (node_a, node_b) into a node
        with adjacency list ({out_links = [a,b,c], page_rank = pr})
        :param node_id:
        :param nodes:
        :return:
        """
        node = dict()
        node['out_links'] = list(nodes)
        node['page_rank'] = 1 / self.options.n_nodes
        yield node_id, node
        for out_node_id in node['out_links']:
            yield out_node_id, "*"

    def convert_edge_list_to_adjacency_list_reducer2(self, node_id, nodes):
        """
        converts the edge list (node_a, node_b) into a node
        with adjacency list ({out_links = [a,b,c], page_rank = pr})
        :param node_id:
        :param nodes:
        :return:
        """
        real_node = None
        for node in nodes:
            if node == "*":
                pass
            elif isinstance(node, dict):
                real_node = node
            else:
                raise ValueError("Shouldn't happen")

        if real_node is None:
            real_node = dict()
            real_node['out_links'] = []
            real_node['page_rank'] = 1 / self.options.n_nodes

        yield node_id, real_node

    def steps(self):
        steps = [MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer1)] + \
                [MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer2)]
        return steps


if __name__ == '__main__':
    MRPageRank.run()
