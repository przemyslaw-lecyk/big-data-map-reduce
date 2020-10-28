#!/usr/bin/env python
from mrjob.job import MRJob


class TestJob(MRJob):
    def __init__(self, *args, **kwargs):
        super(TestJob, self).__init__(*args, **kwargs)

    def mapper(self, _, line):
        # Mapper will either get a record from main or join table
        try:  # See if it is main table record
            (idmovies, title, getner) = line.split(',', 2)
            try:
                idmovies = int(idmovies)
            except:
                idmovies = 0
            yield idmovies, title
        except ValueError:
            try:  # See if it is a join table record
                (userId, idmovies, rating, timestamp) = line.split(",")
                try:
                    rating = float(rating)
                except:
                    rating = 0
                yield idmovies, (rating)
            except ValueError:
                pass  # Record did not match either so skip the record

    # right join
    def reducer(self, key, values):
            yield key, values

    # left join
    # def reducer(self, key, values):
    #     loc = None
    #     for product, sale, location in values:
    #         if location: loc = location
    #     yield key, (product, sale, loc)

    # inner join
    # def reducer(self, key, values):
    #     loc = None
    #     for product, sale, location in values:
    #         if location:
    #             loc = location
    #         elif loc:
    #             yield key, (product, sale, loc)


if __name__ == '__main__':
    TestJob.run()
