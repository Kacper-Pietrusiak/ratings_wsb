from mrjob.job import MRJob

class Avg(MRJob):
    def mapper(self, _, line):
        if line.startswith('userId'):
            return

        parts = line.split(",")
        if len(parts) != 4:
            return

        userId, movieId, rating, timestamp = parts
        try:
            yield movieId, (float(rating), 1)
        except ValueError:
            return

    def reducer(self, movieId, values):
        total_rating = 0
        total_count = 0
        for rating, count in values:
            total_rating += rating
            total_count += count
        average_rating = total_rating / total_count if total_count else 0
        yield movieId, round(average_rating, 2)

if __name__ == '__main__':
    Avg.run()
