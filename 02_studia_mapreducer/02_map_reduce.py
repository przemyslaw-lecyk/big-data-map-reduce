from mrjob.job import MRJob
# from mrjob.step import MRStep


class MRHotelRaitingAvg(MRJob):

    # def steps(self):
    #     return [
    #         MRStep(mapper=self.mapper(),
    #                reducer=self.reducer()
    #                )
    #     ]

    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        rating = float(rating)
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
