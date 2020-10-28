from mrjob.job import MRJob
from mrjob.step import MRStep
import re

SPLIT_RE = re.compile(r',.[^, ]')

class MRHotelRaitingAvg(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapperMovies,
                   # reducer=self.reducer
                   )
        ]


    def mapperMovies(self, _, line):
        (idmovies, title, getner) = line.split(',', 2)
        try:
            idmovies = int(idmovies)
        except:
            idmovies = 0
        yield idmovies, title

    # def reducer(self, key, values):
    #     result = key, values
    #     yield result


if __name__ == '__main__':
    MRHotelRaitingAvg.run()
