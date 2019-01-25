from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict, Counter


class MRWordBigramProb(MRJob):
    def mapper(self, joke_id, content):
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            bigram_count = defaultdict(lambda: 0)
            if index < len(words) - 1:
                next_word = words[index + 1]
            else:  # the last word
                next_word = ''
            bigram_count[next_word] += 1
            yield word, bigram_count


    def reducer(self, word, stripes):
        c = Counter()
        for stripe in stripes:
            c.update(stripe)
        count_total = sum(c.values())
        del c['']
        for key in c:
            yield (word + "_" + key), (c[key] / count_total)

## check that all probabilities add up to 1

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    MRWordBigramProb.run()
