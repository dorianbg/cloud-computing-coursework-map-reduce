from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict
import operator


class MRWordBigramProb(MRJob):
    MRJob.PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def mapper_init(self):
        """
        Initialises the dictionary used for "in-mapper" combining

        :return:
        """
        self.pair_counts = defaultdict(lambda: 0)

    def mapper(self, joke_id, content):
        """
        For each encountered word in joke, it increments the count for the bigram

        with it's following word.

        it also adds the value to "*"

        Note that we ignore the last word in a sentence as it has no following words.

        :param joke_id: ignored
        :param content: the raw content of the document
        :return: tuple of ((word, following_word),1) or ((word,*))
        """
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            if index < len(words) - 1:
                self.pair_counts[(word, "*")] += 1  # will be used to calculated the total number of occurrences of the word
                self.pair_counts[(word, words[index+1])] += 1

    def mapper_final(self):
        """
        Yields the pairs after they have combined.

        :return:
        """
        for pair in self.pair_counts.keys():
            yield pair, self.pair_counts[pair]

    def reducer_init(self):
        """
        Keeps state over multiple reduce iterations, very important in order to
        calculate the "total_word_count" for each 1st word of the bigram

        :return:
        """
        self.total_word_count = {}
        self.bigram_frequencies = []

    def reducer(self, pair, counts):
        """
        Expects that the input pair (word, following_word) is partitioned by 1st key (word)
        and sorted by the 2nd key (following word).
        It expects that "*" will be the first following word for every word.
        This is exactly specified in the config.

        Outputs a triple containing (word, following word, conditional probability).

        :param pair: a bigram (word, following_word)
        :param counts: sum of counts for every bigram on every node
        :return:
        """
        word = pair[0]
        following_word = pair[1]
        if following_word == "*":
            self.total_word_count[word] = sum(counts)
        else:
            self.bigram_frequencies.append((word, following_word, (sum(counts) / self.total_word_count[word])))

    def reducer_final(self):
        """
        Sorts the triples of (word, following word, frequency of the combination)
        based on 1st word and then frequency of the bigram (word, following word).
        Outputs sorted values.

        :return:
        """
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
            'mapred.text.key.comparator.options': '-k2,2r',
            'mapred.text.key.partitioner.options': '-k1,1',
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
