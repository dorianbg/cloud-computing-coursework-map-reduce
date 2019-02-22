#!/usr/bin/env bash
# general
mrjob create-cluster --max-hours-idle 1


# bigram stripes
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/bigram_prob_stripes.py \
s3://mapreduce123443/data/shortjokes.csv \
-r emr \
--cluster-id j-2468K83D5JO9F \
--output-dir=s3://mapreduce123443/output/bigram_prob_stripes3

# bigram pairs
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/bigram_prob_pairs.py \
s3://mapreduce123443/data/shortjokes.csv \
-r emr \
--cluster-id j-2468K83D5JO9F \
--output-dir=s3://mapreduce123443/output/bigram_prob_pairs3

# page rank
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/page_rank.py \
s3://mapreduce123443/data/soc-Epinions1.txt \
-r emr \
--cluster-id j-2468K83D5JO9F \
--n_iterations=20 \
--n_nodes=75879 \
--top_n=100 \
--no-read-logs \
--output-dir=s3://mapreduce123443/output/epinions_pagerank_simple3

# page rank complete
cd /Volumes/SD/PyCharmProjects/cc_coursework;
python /Volumes/SD/PyCharmProjects/cc_coursework/page_rank_complete.py \
s3://mapreduce123443/data/soc-Epinions1.txt \
-r emr \
--cluster-id j-2468K83D5JO9F \
--n_iterations=20 --n_nodes=75879 \
--min_node_id=0 --max_node_id=75887 \
--damping_factor=0.85 --top_n=100 \
--no-read-logs \
--output-dir=s3://mapreduce123443/output/epinions_pagerank_complete3
