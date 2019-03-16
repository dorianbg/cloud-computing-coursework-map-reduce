#!/usr/bin/env bash
# general
mrjob create-cluster --max-hours-idle 1


# bigram stripes
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/bigram_prob_stripes.py \
s3://mapreduce123443/data/shortjokes.csv \
-r emr \
--cluster-id j-3S07C4STGBV7Z \
--output-dir=s3://mapreduce123443/output/bigram_prob_stripes_22feb_1

# bigram pairs
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/bigram_prob_pairs.py \
s3://mapreduce123443/data/shortjokes.csv \
-r emr \
--cluster-id j-3S07C4STGBV7Z \
--output-dir=s3://mapreduce123443/output/bigram_prob_pairs_final


# page rank
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/page_rank.py \
s3://mapreduce123443/data/soc-Epinions1.txt \
-r emr \
--cluster-id j-3S07C4STGBV7Z \
--n_iterations=10 \
--n_nodes=75879 \
--top_n=80000 \
--no-read-logs \
--output-dir=s3://mapreduce123443/output/epinions_pagerank_simple_final

# page rank complete
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/page_rank_complete.py \
s3://mapreduce123443/data/soc-Epinions1.txt \
-r emr \
--cluster-id j-3S07C4STGBV7Z \
--n_iterations=50 --n_nodes=75879 \
--min_node_id=0 --max_node_id=75887 \
--damping_factor=0.85 --top_n=80000 \
--no-read-logs \
--output-dir=s3://mapreduce123443/output/epinions_pagerank_complete_final
