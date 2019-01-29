from mrjob.job import MRJob, MRStep
from mrjob.protocol import JSONValueProtocol, RawValueProtocol

class MRPageRank(MRJob):

    INPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCL = JSONValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg(
            '--n_iterations', type=int, default=50, help='Number of iterations of page rank to run')
        self.add_passthru_arg(
            '--damping_factor', type=float, default=0.85, help='The damping factor using in the calculations')

    '''
    def total_number_of_nodes(self):
        
    '''

    def reducer_convert_edge_list(self, node_id, nodes):
        node = {}
        node['out_links'] = nodes
        node['page_rank'] = 1
        yield node_id, node

    def mapper(self, node_id, node):
        # 1. calculate my Page rank
        # 2. divide the PageRank by total number of outgoing
        # 3. emit the node with page rank
        # 4. emit for every outgoing edge the page rank +
        yield node_id, node
        page_rank_contribution = node['page_rank']/len(node['out_links'])
        for out_link_node_id in node['out_links']:
            yield out_link_node_id, page_rank_contribution

    def reducer(self, node_id, page_rank_contributions):
        node = None
        page_rank_sum = 0
        for page_rank_contribution in page_rank_contributions:
            if page_rank_contribution.get('page_rank', ' ') != ' ':
                node = page_rank_contribution
            else:
                page_rank_sum += page_rank_contribution
        node['page_rank'] = self.options.damping_factor + (1-self.options.damping_factor) * page_rank_sum
        yield node_id, node

    def steps(self):
        return [
            MRStep(reducer=self.reducer_convert_edge_list),
            MRStep(mapper=self.mapper,
                   reducer=self.reducer) * self.options.n_iterations
        ]


if __name__ == '__main__':
    MRPageRank.run()
