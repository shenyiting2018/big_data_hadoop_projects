from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (location, date, temp_type, temp, _, _, _, _) = line.split(',')
        if temp_type == 'TMIN':
            yield location, temp

    def reducer(self, location, temps):
        yield location, min(temps)
        
        
if __name__ == '__main__':
    MRRatingCounter.run()
