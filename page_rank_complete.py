from mrjob.job import MRJob, MRStep
from mrjob.protocol import TextProtocol
from collections import defaultdict
import heapq


class MRPageRank(MRJob):
    INPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        """
        We configure:
        - n_iterations - number of iterations of page rank
        - damping_factor - the damping factor from the page rank equation
        - n_nodes - number of nodes in the graph
        - min_node_id - the minimal node id
        - max_node_id - the maximal node id
        - top_n - how many of the top results we want

        :return:
        """

        super(MRPageRank, self).configure_args()
        self.add_passthru_arg(
            '--n_iterations', type=int, default=10, help='Number of iterations of page rank to run')
        self.add_passthru_arg(
            '--damping_factor', type=float, default=0.85, help='The damping factor using in the calculations')
        self.add_passthru_arg(
            '--n_nodes', type=int, default=75879, help='Number of nodes in the graph (computed using count_nodes.py)')
        self.add_passthru_arg(
            '--min_node_id', type=int, default=0, help='Minimal node id (computed using count_nodes.py)')
        self.add_passthru_arg(
            '--max_node_id', type=int, default=75887, help='Maximal node id (computed using count_nodes.py)')
        self.add_passthru_arg(
            '--top_n', type=int, default=10, help='Number of top pages to output')

    def convert_edge_list_to_adjacency_list_reducer(self, node_id, nodes):
        """
        Converts the edge list (node_a, node_b) into a single node
        with adjacency list ({out_links = [a,b,c], page_rank = pr})

        Furthermore, we output all outgoing nodes with a placeholder "*"
        to initialise the dangling nodes in the following reducer

        :param node_id: the node id which is the key in reduce step
        :param nodes: list of node id's
        :return:
        """
        node = dict()
        node['out_links'] = list(nodes)
        node['page_rank'] = 1/self.options.n_nodes
        yield node_id, node
        for out_node_id in node['out_links']:
            yield out_node_id, "*"

    def convert_edge_list_to_adjacency_list_reducer_dangling(self, node_id, nodes):
        """
        The previous reducer outputs a node id and either "*" or the nodes structure as value.

        This reducer doesn't change the nodes that have a node structure as value.
        For nodes that only have "*" as value, it constructs a dangling node.

        :param node_id: the node id which is the key in reduce step
        :param nodes: list "*" or node structure
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

    def map_page_rank_contribution_init(self):
        """
        To utilise the "in-mapper" combiner pattern, initialises a dictionary
        for holding nodes and their associated sums of incoming page ranks:
            - {node_id: [sum of incoming page rank contributions] , ...}

        :return:
        """
        self.incoming_page_ranks = defaultdict(lambda: 0)
        self.dangling_pr_mass = 0

    def map_page_rank_contribution(self, node_id, node):
        """
        For every node id and it's associated node structure:
        1.
        A) if it's not  dangling node:
            1. divides the page rank of the node by the total number of outgoing links
            2. stores the page rank contribution to every outgoing link (node)
                which later gets emited using the "in-mapper" combiner patter
        B) if it's a dangling node
            1. store it's mass together with the mass of other dangling nodes
        2. emits the inputs node_id and node_structure

        :param node_id: node id
        :param node: node structure in JSON format
        :return:
        """
        if len(node['out_links']) > 0:
            page_rank_contribution = node['page_rank'] / len(node['out_links'])
            for out_link_node_id in node['out_links']:
                self.incoming_page_ranks[out_link_node_id] += page_rank_contribution
        elif len(node['out_links']) == 0:
            self.dangling_pr_mass += node['page_rank']
        yield node_id, node

    def map_page_rank_contribution_final(self):
        """
        For every stored node_id, returns combined incoming page rank contributions.

        Returns the page rank mass of all dangling nodes in this reducer using a special "d" key
        :return:
        """
        for node_id in self.incoming_page_ranks.keys():
            yield node_id, self.incoming_page_ranks[node_id]
        yield 'd', self.dangling_pr_mass

    def reduce_incoming_page_rank_contributions(self, node_id, page_rank_contributions):
        """
        1)
            A) If the incoming node is the sum of dangling masses,
                then it loops through the range on all node id's and emits a page rank contribution to each node
            B) If the node is a normal node, it just sums up the contributions, updates the nodes pages rank
                and outputs the node structure

        :param node_id: node id
        :param page_rank_contributions: list of contributions -> either ("d",<mass>), (<node id>, <mass>), (<node id>, <node structure>)
        :return:
        """
        if node_id == 'd':
            dangling_mass = sum(page_rank_contributions)
            for node_id in range(self.options.min_node_id, self.options.max_node_id):
                yield str(node_id), dangling_mass
        else:
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

    def complete_page_rank_reducer(self, node_id, dangling_nodes_contributions):
        """
        1. Sums up all the incoming contributions from the dangling nodes (emitted in previous reducer)
        2. Properly updates the pagerank to take into account the damping factor and dangling mass

        :param node_id:
        :param dangling_nodes_contributions: either (<node id>, <dangling mass contribution>), (<node id>, <node structure>)
        :return:
        """
        node = None
        dangling_contribution = 0
        for i, dangling_nodes_contribution in enumerate(dangling_nodes_contributions):
            if isinstance(dangling_nodes_contribution, float):
                dangling_contribution += dangling_nodes_contribution
            else:
                # this is the case where we send the actual node JSON structure
                node = dangling_nodes_contribution

            # dangling nodes do not have the graph node structure

        if node is not None:
            old_page_rank = node['page_rank']
            updated_page_rank = ((1 - self.options.damping_factor) / self.options.n_nodes) \
                + (self.options.damping_factor * (dangling_contribution / self.options.n_nodes + old_page_rank))
            node['page_rank'] = updated_page_rank

            yield node_id, node
        else:
            # these are the 9 missing node id's in the range min_node_id to max_node_id
            pass

    def topN_mapper_init(self):
        """
        Stores all the page ranks received in the mapper

        :return:
        """
        self.values = []

    def topN_mapper(self, node_id, node):
        """
        Reverts the order so that page ranks are keys.
        This is used together with a custom comparator in the shuffle stage before the next reducer

        :param node_id:
        :param node:
        :return:
        """
        self.values.append((node['page_rank'], node_id))

    def topN_mapper_final(self):
        """
        Returns N largest values based on page rank as (page rank, node id)

        :return:
        """
        for pair in heapq.nlargest(n=self.options.top_n, iterable=self.values, key=lambda x: x[0]):
            yield pair

    def topN_reducer_init(self):
        """
        Keeps track of how many top page ranks were iterated over

        :return:
        """
        self.values = []

    def topN_reducer(self, page_rank, node_ids):
        """
        Outputs the top N nodes along with their page ranks

        :param page_rank:
        :param node_ids:
        :return:
        """
        for node_id in node_ids:
            self.values.append((page_rank, node_id))

    def topN_reducer_final(self):
        """
        Returns N largest values based on page rank as (page rank, node id)

        :return:
        """
        for pair in heapq.nlargest(n=self.options.top_n, iterable=self.values, key=lambda x: x[0]):
            yield pair

    def steps(self):
        steps = [MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer)] \
                + [ MRStep(reducer=self.convert_edge_list_to_adjacency_list_reducer_dangling)] \
                + [
                    MRStep(
                        mapper_init=self.map_page_rank_contribution_init,
                        mapper=self.map_page_rank_contribution,
                        mapper_final=self.map_page_rank_contribution_final,
                        reducer=self.reduce_incoming_page_rank_contributions
                    ),
                    MRStep(
                        reducer=self.complete_page_rank_reducer)
                ] * self.options.n_iterations \
                + [MRStep(mapper_init=self.topN_mapper_init,
                        mapper=self.topN_mapper,
                        mapper_final=self.topN_mapper_final,
                        reducer_init=self.topN_reducer_init,
                        reducer=self.topN_reducer,
                        reducer_final=self.topN_reducer_final,
                        jobconf={
                            'mapred.reduce.tasks': 1,
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1nr'
                        })
                 ]

        return steps


if __name__ == '__main__':
    MRPageRank.run()
