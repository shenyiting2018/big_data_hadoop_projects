from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (_, name, age, num_friends) = line.split(',')
        yield age, int(num_friends) 

    def reducer(self, age, num_friends_list):
        total = 0
        num_elements = 0
        for num_friends in num_friends_list:
            total += num_friends
            num_elements += 1
        yield age, total / num_elements

if __name__ == '__main__':
    MRRatingCounter.run()
