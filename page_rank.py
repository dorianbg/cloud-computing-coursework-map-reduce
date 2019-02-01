from mrjob.job import MRJob, MRStep
from mrjob.protocol import TextProtocol


class MRPageRank(MRJob):
    INPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg(
            '--n_iterations', type=int, default=10, help='Number of iterations of page rank to run')
        self.add_passthru_arg(
            '--damping_factor', type=float, default=0.85, help='The damping factor using in the calculations')

    def convert_edge_list_to_adjacency_list(self, node_id, nodes):
        """
        converts the edge list (node_a, node_b) into a node
        with adjacency list ({out_links = [a,b,c], page_rank = 5})
        :param node_id:
        :param nodes:
        :return:
        """
        node = dict()
        node['out_links'] = list(nodes)
        node['page_rank'] = 5
        yield node_id, node

    def map_page_rank_contribution(self, node_id, node):
        """
        1. divide my PageRank by total number of outgoing links
        2. emit the page rank contribution to every node
        3. emit for every outgoing edge the page rank +
        :param node_id:
        :param node:
        :return:
        """
        yield node_id, node
        page_rank_contribution = node['page_rank']/len(node['out_links'])
        for out_link_node_id in node['out_links']:
            yield out_link_node_id, page_rank_contribution

    def reduce_incoming_page_rank_contributions(self, node_id, page_rank_contributions):
        """
        reduce all incoming page rank contributions for a node
        :param node_id:
        :param page_rank_contributions:
        :return:
        """
        node = None
        page_rank_sum = 0
        for page_rank_contribution in page_rank_contributions:
            if isinstance(page_rank_contribution, float):
                page_rank_sum += page_rank_contribution
            else:
                node = page_rank_contribution
        if node is not None:
            node['page_rank'] = self.options.damping_factor + (1-self.options.damping_factor) * page_rank_sum
            yield node_id, node

    def top_mapper(self, node_id, node):
        yield node['page_rank'], node_id

    def top_reducer(self, page_rank, node_ids):
        for node in node_ids:
            yield node, page_rank

    def steps(self):
        steps = [MRStep(reducer=self.convert_edge_list_to_adjacency_list)] + \
                [MRStep(mapper=self.map_page_rank_contribution,
                        reducer=self.reduce_incoming_page_rank_contributions)] * self.options.n_iterations + \
                [MRStep(mapper=self.top_mapper, reducer=self.top_reducer)]
        return steps


if __name__ == '__main__':
    MRPageRank.run()
