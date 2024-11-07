from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgMovieTitle(MRJob):
    def configure_args(self):
        super(AvgMovieTitle, self).configure_args()
        self.add_file_arg('--movies', help="Path to the movies.csv file")

    def steps(self):
        return [
            MRStep(mapper=self.mapper_ratings,
                   reducer=self.reducer_avg_rating),
            MRStep(mapper_init=self.mapper_init_movies,
                   mapper=self.mapper_join,
                   reducer=self.reducer_join)
        ]

    def mapper_ratings(self, _, line):
        if line.startswith('userId'):
            return

        parts = line.split(",")
        if len(parts) != 4:
            return

        userId, movieId, rating, timestamp = parts
        try:
            yield int(movieId), (float(rating), 1)
        except ValueError:
            return

    def reducer_avg_rating(self, movieId, values):
        total_rating = 0
        total_count = 0
        for rating, count in values:
            total_rating += rating
            total_count += count
        average_rating = total_rating / total_count if total_count else 0
        yield movieId, round(average_rating, 2)

    def mapper_init_movies(self):
        # Określamy kodowanie UTF-8
        self.movie_titles = {}
        with open(self.options.movies, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('movieId'):
                    continue
                parts = line.strip().split(",", 2)
                if len(parts) >= 2:
                    movieId = int(parts[0])
                    title = parts[1]
                    self.movie_titles[movieId] = title

    def mapper_join(self, movieId, avg_rating):
        title = self.movie_titles.get(movieId, "Unknown Title")
        yield movieId, (title, avg_rating)

    def reducer_join(self, movieId, values):
        for title, avg_rating in values:
            yield title, avg_rating

if __name__ == '__main__':
    AvgMovieTitle.run()
