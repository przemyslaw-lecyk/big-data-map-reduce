#!/usr/bin/env python
from mrjob.job import MRJob


class MoviesRatingsJob(MRJob):
    def __init__(self, *args, **kwargs):
        super(MoviesRatingsJob, self).__init__(*args, **kwargs)

    def mapper(self, _, line):
        # Mapper will either get a record from main or join table
        try:  # See if it is main table record
            movieId, title = line.split(',', 1)
            yield movieId, (title, '', '', '')
        except ValueError:
            try:  # See if it is a join table record
                userId, movieId, rating, timestamp = line.split(',')
                yield movieId, ('', rating, userId, timestamp)
            except ValueError:
                pass  # Record did not match either so skip the record

    def reducer(self, key, values):
        tit = None
        for title, rating, userId, timestamp in values:
            if title:
                tit = title
            else:
                yield key, (tit, rating, userId, timestamp)

    # def reducer(self, key, values):
    #     loc = None
    #     for product, sale, location in values:
    #         if location: loc = location
    #         else: yield key, (product, sale, loc)


if __name__ == '__main__':
    MoviesRatingsJob.run()
