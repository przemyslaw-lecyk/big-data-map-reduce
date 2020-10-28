from mrjob.job import MRJob
from mrjob.step import MRStep


class MRHotelRaitingAvg(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapperRatings,
                   reducer=self.reducer
                   )
        ]

    def mapperRatings(self, _, line):
        (userId, HName, rating, timestamp) = line.split(",")
        try:
            rating = float(rating)
        except:
            rating = 0
        yield HName, (rating)

    def reducer(self, key, values):
        total = 0
        num_elements = 0
        for values in values:
            total += values
            num_elements += 1
        result = [key, total / num_elements]
        yield result


if __name__ == '__main__':
    MRHotelRaitingAvg.run()
