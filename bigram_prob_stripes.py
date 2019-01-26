from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict, Counter


class MRWordBigramProb(MRJob):
    def mapper(self, joke_id, content):
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")

        following_words = defaultdict(lambda: [])
        for index, word in enumerate(words):
            if index < len(words) - 1:
                following_words[word].append(words[index+1])

        for original_word in following_words.keys():
            bigram_count = defaultdict(lambda: 0)
            for following_word in following_words[original_word]:
                bigram_count[following_word] += 1
            yield original_word, bigram_count

    def combiner(self, key, stripes):
        combined_stripes = Counter()
        for stripe in stripes:
            combined_stripes.update(stripe)
        # result in eg ( word, {'following_word' : 1}, word, {'following_word' : 2} )
        yield key, combined_stripes

    def reducer(self, word, stripes):
        c = Counter()
        for stripe in stripes:
            c.update(stripe)
        count_total = sum(c.values())
        for key in c:
            yield (word + "_" + key), (c[key] / count_total)

        ## check that all probabilities add up to 1

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    MRWordBigramProb.run()
