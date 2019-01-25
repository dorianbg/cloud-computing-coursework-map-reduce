from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict, Counter


class MRWordBigramProb(MRJob):
    def mapper(self, joke_id, content):
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            if index < len(words) - 1:
                next_word = words[index + 1]
            else:  # the last word
                next_word = ''
            bigram_count[next_word] += 1
            yield (word, next_word), 1

    ## needs a partitioner + a sorter
    # we need to yield - (word,'') to count the N of word
    # we need to yield - (word, word') to count the N of word,word' combinations
    def reducer(self, pair, count):
        c = Counter()
        for stripe in stripes:
            c.update(stripe)
        count_total = sum(c.values())
        del c['']
        for key in c:
            yield (word + "_" + key), (c[key] / count_total)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    MRWordBigramProb.run()
