from mrjob.job import MRJob
import re

class MRMoviesRaitingAvg(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRMoviesRaitingAvg, self).__init__(*args, **kwargs)

    def mapper(self, _, line):
        # Mapper will either get a record from main or join table
        try:  # See if it is main table record
            (userId, movie_id, rating, timestamp) = line.split(',')
            try:
                rating = float(rating)
            except:
                rating = 0
            yield movie_id, (rating, '', '')
        except ValueError:
            try:  # See if it is a join table record
                movie_id, title = line.split(",", 1)
                yield movie_id, ('', '',  title)
            except ValueError:
                pass  # Record did not match either so skip the record

    # right join
    def reducer(self, key, values):
        loc = None
        for product, rating, title in values:
             if title:
                 loc = title
             else:
                 yield key, (product, rating, loc)
    # left join
    # def reducer(self, key, values):
    #     loc = None
    #     for product, sale, title in values:
    #         if title: loc = title
    #     yield key, (product, sale, loc)

    # def reducer(self, key, values):
    #     total = 0
    #     num_elements = 0
    #     for values in values:
    #         total += values
    #         num_elements += 1
    #     result = [self.movies_names[key], total / num_elements]
    #     yield result


if __name__ == '__main__':
    MRMoviesRaitingAvg.run()
