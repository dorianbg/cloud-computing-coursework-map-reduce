from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict
import operator


class MRWordBigramProb(MRJob):
    MRJob.PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def mapper_init(self):
        self.pair_counts = defaultdict(lambda: 0)

    def mapper(self, joke_id, content):
        """
        for each encountered word in the creates a
        ignore the last word in a sentence as it has not following words.

        :param joke_id:
        :param content:
        :return: tuple of ((word, following_word),1) or ((word,*))
        """
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            if index < len(words) - 1:
                self.pair_counts[(word, "*")] += 1
                self.pair_counts[(word, words[index+1])] += 1

    def mapper_final(self):
        for pair in self.pair_counts.keys():
            yield pair, self.pair_counts[pair]

    def reducer_init(self):
        """
        keeps state over multiple reduce iterations
        :return:
        """
        self.total_word_count = {}
        self.bigram_frequencies = []

    def reducer(self, pair, counts):
        """
        does the final calculation
        :param pair:
        :param counts:
        :return:
        """
        word = pair[0]
        following_word = pair[1]
        if following_word == "*":
            self.total_word_count[word] = sum(counts)
        else:
            self.bigram_frequencies.append((word, following_word, (sum(counts) / self.total_word_count[word])))

    def reducer_final(self):
        sorted_bigram_frequencies = sorted(self.bigram_frequencies, key=operator.itemgetter(0, 2))
        for bigram in sorted_bigram_frequencies:
            yield bigram[0] + "-" + bigram[1], bigram[2]

    def steps(self):
        """
        jobconf = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k 1',
            'mapred.text.key.partitioner.options': '-k 2',

            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2r',
        }
        """
        jobconf = {
            'stream.num.map.output.key.fields': '2',
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1,1',
            'mapred.text.key.partitioner.options': '-k2,2r',
        }
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final,
                   jobconf=jobconf
            )
        ]


if __name__ == '__main__':
    MRWordBigramProb().run()
