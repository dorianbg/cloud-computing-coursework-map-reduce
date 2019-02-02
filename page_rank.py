from mrjob.job import MRJob, MRStep
from mrjob.protocol import TextProtocol
from collections import defaultdict


class MRPageRank(MRJob):
    INPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg(
            '--n_iterations', type=int, default=10, help='Number of iterations of page rank to run')
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
        node['page_rank'] = 1/self.options.n_nodes
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
            real_node['page_rank'] = 1/self.options.n_nodes

        yield node_id, real_node

    def mapper_init(self):
        """
        initialised a dictionary holding {node_id: [sum of incoming page rank contributions] , ...}
        :return:
        """
        self.page_ranks = defaultdict(lambda: 0)

    def map_page_rank_contribution(self, node_id, node):
        """
        1. divides nodes PageRank by total number of outgoing links
        2. emits the page rank contribution to every outgoing link (node)
        :param node_id: node id
        :param node: node structure in JSON format
        :return:
        """
        if len(node['out_links']) > 0:
            page_rank_contribution = node['page_rank'] / len(node['out_links'])
            for out_link_node_id in node['out_links']:
                self.page_ranks[out_link_node_id] += page_rank_contribution
        yield node_id, node

    def mapper_final(self):
        """
        returns combined incoming page rank contributions for every node id
        :return:
        """
        for node_id in self.page_ranks.keys():
            yield node_id, self.page_ranks[node_id]

    def reduce_incoming_page_rank_contributions(self, node_id, page_rank_contributions):
        """
        sums up all incoming page rank contributions for a node without taking into account the
        loss of page rank mass due to dangling nodes nor the "teleportation"
        :param node_id: node id
        :param page_rank_contributions: list of contribution values
        :return:
        """
        node = None
        page_rank_sum = 0
        for page_rank_contribution in page_rank_contributions:
            if isinstance(page_rank_contribution, float):
                page_rank_sum += page_rank_contribution
            else:
                # this is the case where we send the actual node JSON structure
                node = page_rank_contribution

        node['page_rank'] = page_rank_sum
        yield node_id, node

    def top_mapper(self, node_id, node):
        """
        reverts the order so that page ranks are keys.
        this is used together with a custom comparator
        :param node_id:
        :param node:
        :return:
        """
        yield node['page_rank'], node_id

    def init_reducer(self):
        """
        keeps track of how many page ranks were computed
        :return:
        """
        self.top_n_counter = 0

    def top_reducer(self, page_rank, node_ids):
        """
        outputs the resulting nodes along with their page ranks
        :param page_rank:
        :param node_ids:
        :return:
        """
        for node in node_ids:
            if self.top_n_counter < 200000:
                yield node, page_rank
                self.top_n_counter += 1

    def steps(self):
        steps = [MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer1)] + \
                [MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer2)] + \
                [MRStep(
                    mapper_init=self.mapper_init,
                    mapper=self.map_page_rank_contribution,
                    mapper_final=self.mapper_final,
                    reducer=self.reduce_incoming_page_rank_contributions)
                ] * self.options.n_iterations + \
                [MRStep(mapper=self.top_mapper,
                    reducer_init=self.init_reducer,
                    reducer=self.top_reducer,
                    jobconf={
                        'mapred.reduce.tasks': 1,
                        'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                        'mapred.text.key.comparator.options': '-k1,1nr'
                    })
                ]
        return steps


if __name__ == '__main__':
    MRPageRank.run()
