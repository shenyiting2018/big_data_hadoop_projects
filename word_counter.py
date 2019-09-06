from mrjob.job import MRJob
import re


WORD_REGEXP = re.compile(r"[\w']+")


class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, count):
        yield word, sum(count)

if __name__ == '__main__':
    MRRatingCounter.run()
