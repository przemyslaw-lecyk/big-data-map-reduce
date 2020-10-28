# Uruchamianie: python 03_map_reduce.py ratings.csv --movies movies.csv

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMoviesRaitingAvg(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer_init = self.reducer_init,
                   reducer=self.reducer
                   )
        ]

    def configure_args(self):
        super(MRMoviesRaitingAvg, self).configure_args()
        self.add_file_arg('--movies', help = 'Ścieżka do pliku movies.csv')


    def mapper(self, _, line):
        (userId, movieId, rating, timestamp) = line.split(',')
        try:
            rating = float(rating)
        except:
            rating = 0
        yield movieId, (rating)

    def reducer_init(self):
        self.movies_names = {}
        with open('movies.csv','r') as file:
            for line in file:
                movie_ID, full_name, genter = line.split(',', 2)
                # full_name = full_name
                self.movies_names[movie_ID] = full_name


    def reducer(self, key, values):
        total = 0
        num_elements = 0
        for values in values:
            total += values
            num_elements += 1
        result = [self.movies_names[key], total / num_elements]
        yield result


if __name__ == '__main__':
    MRMoviesRaitingAvg.run()
