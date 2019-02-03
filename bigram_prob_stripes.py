from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict, Counter
import operator


class MRWordBigramProb(MRJob):
    def mapper_init(self):
        """
        initialise the dictionary for "in-mapper" combining
        :return:
        """
        self.following_words = defaultdict(lambda: [])

    def mapper(self, joke_id, content):
        """
        for each encountered word in the text add a entry to the list of following words.
        ignore the last word in a sentence as it has not following words.
        :param joke_id:
        :param content:
        :return:
        """
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            if index < len(words) - 1:
                self.following_words[word].append(words[index+1])

    def mapper_final(self):
        """
        create a stripe out of the list of the succeeding words for each word
        :return:
        """
        for original_word in self.following_words.keys():
            bigram_count = defaultdict(lambda: 0)
            for following_word in self.following_words[original_word]:
                bigram_count[following_word] += 1
            yield original_word, bigram_count

    def reducer_init(self):
        self.bigram_frequencies = []

    def reducer(self, word, stripes):
        """

        :param word:
        :param stripes:
        :return:
        """
        counts = Counter()
        for stripe in stripes:
            counts.update(stripe)
        count_total = sum(counts.values())
        for following_word in counts:
            self.bigram_frequencies.append((word, following_word, (counts[following_word] / count_total)))

    def reducer_final(self):
        sorted_bigram_frequencies = sorted(self.bigram_frequencies, key=operator.itemgetter(0, 2))
        for bigram in sorted_bigram_frequencies:
            yield bigram[0] + "-" + bigram[1], bigram[2]

    def steps(self):
        steps = [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final
            )
        ]
        return steps


if __name__ == '__main__':
    MRWordBigramProb.run()
