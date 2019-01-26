from mrjob.job import MRJob, MRStep
import re
from collections import defaultdict, Counter


class MRWordBigramProb(MRJob):
    def mapper_init(self):
        self.following_words = defaultdict(lambda: [])

    def mapper(self, joke_id, content):
        words = re.sub('\s+', ' ', re.sub('[^a-z]+', ' ', re.sub('\'', '', content.lower()))).strip(' ').split(" ")
        for index, word in enumerate(words):
            if index < len(words) - 1:
                self.following_words[word].append(words[index+1])

    def mapper_final(self):
        for original_word in self.following_words.keys():
            bigram_count = defaultdict(lambda: 0)
            for following_word in self.following_words[original_word]:
                bigram_count[following_word] += 1
            yield original_word, bigram_count

    def reducer(self, word, stripes):
        c = Counter()
        for stripe in stripes:
            c.update(stripe)
        count_total = sum(c.values())
        for key in c:
            yield (word + "_" + key), (c[key] / count_total)  # all probabilities add up to 1

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    MRWordBigramProb.run()
