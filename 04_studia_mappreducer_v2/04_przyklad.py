#!/usr/bin/env python
from mrjob.job import MRJob

class MoviesRatingsJob(MRJob):
    def __init__(self, *args, **kwargs):
        super(MoviesRatingsJob, self).__init__(*args, **kwargs)

    def mapper(self, _, line):
        # Mapper will either get a record from main or join table
        try: # See if it is main table record
            name, product, sale = line.split(',')
            yield name, (product, int(sale), '')
        except ValueError:
            try: # See if it is a join table record
                name, location = line.split(',')
                yield name, ('', '', location)
            except ValueError:
                pass # Record did not match either so skip the record

    def reducer(self, key, values):
        loc = None
        for product, sale, location in values:
            if location: loc = location
            else: yield key, (product, sale, loc)

if __name__ == '__main__':
    MoviesRatingsJob.run()