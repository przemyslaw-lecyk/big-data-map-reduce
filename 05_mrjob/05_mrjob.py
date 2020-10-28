import skip as skip
from mrjob.job import MRJob


# from mrjob.step import MRStep

class MoviesRaiting(MRJob):

    # def steps(self):
    #     return [
    #         MRStep(mapper=self.mapper(),
    #                reducer=self.reducer()
    #                )
    #     ]

    def mapper(self, _, line):
        (userId, movie_id, rating, timestamp) = line.split(",")
        ocena = rating
        yield movie_id, ocena

    def reducer(self, key, values):
        # total = 0
        # num_elements = 0
        # for values in values:
        #     total += values
        #     num_elements += 1
        # result = [key, total / num_elements]
        yield key, values


if __name__ == '__main__':
    MoviesRaiting.run()
