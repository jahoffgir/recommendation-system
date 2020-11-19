from sklearn.feature_extraction.text import CountVectorizer as cv
from sklearn.metrics.pairwise import cosine_similarity
from my_sql import MySQL
import pandas as pd
import numpy as np

"""
Content-based Recommendation System
"""


class ContentBasedRecommendationSystem:
    def __init__(self, watched_movie, mysql):
        self.watched_movie = watched_movie
        self.mysql = mysql
        self.connection = mysql.get_connection()
        # self.movies = self.read_table(
        #     """
        #     SELECT movielenstable.movieId, title, genres, tags.tag
        #     FROM movielenstable JOIN
        #         (
        #             SELECT movieId, GROUP_CONCAT(tag SEPARATOR '|') AS tag
        #             FROM lenstags
        #             GROUP BY movieId
        #         ) AS tags
        #     ON movielenstable.movieId = tags.movieId;
        #     """
        # )
        self.movies = self.read_table(
            """
            SELECT lenslinks.movieId, title, genres, imdbId
            FROM movielenstable JOIN lenslinks ON movielenstable.movieId = lenslinks.movieId
            """
        )
        concat = []
        temp = pd.read_csv("data/actors-movieIds.csv").groupby('imdbId').agg({'personName': ' '.join})
        for index, row in temp.iterrows():
            concat.append([index, row['personName']])

        self.movie_by_actor = pd.DataFrame(np.array(concat), columns=['imdbId', 'actorName'])

        concat = []
        temp = pd.read_csv("data/directors-movieIds.csv").groupby('imdbId').agg({'personName': ' '.join})
        for index, row in temp.iterrows():
            concat.append([index, row['personName']])

        self.movie_by_director = pd.DataFrame(np.array(concat), columns=['imdbId', 'directorName'])

        self.movie_by_actor['imdbId'] = self.movie_by_actor['imdbId'].astype(int)
        self.movie_by_director['imdbId'] = self.movie_by_director['imdbId'].astype(int)
        merged = pd.merge(left=self.movies, right=self.movie_by_actor, left_on='imdbId', right_on='imdbId')
        self.movies = pd.merge(left=merged, right=self.movie_by_director, left_on='imdbId', right_on='imdbId')

    def __del__(self):
        print("Closing db connection")
        self.connection.close()

    def get_movie_title(self, i):
        """
        Gets the movie with the movie id
        :param movieId: movie id
        :return: titie
        """
        for index, row in self.movies.iterrows():
            if i == index:
                return row

    def get_movie_id(self, title):
        """
        Gets title from the movie id
        :param movieId: movie title
        :return: movieId
        """
        for index, row in self.movies.iterrows():
            if row['title'] == title:
                return index

    def train(self):
        """
        Trains the model based on movies; title, genres, tag
        :return:
        """
        for attribute in ['title', 'genres', 'actorName', 'directorName']:
            self.movies[attribute] = self.movies[attribute]
        self.movies['merged'] = self.movies.apply(self.merge, axis=1)
        count_vectorized = cv()
        cs = cosine_similarity(count_vectorized.fit_transform(self.movies['merged']))
        recommended_movies = list(enumerate(cs[self.get_movie_id(self.watched_movie)]))

        if recommended_movies:
            predicted = self.get_highest(recommended_movies)
            for i, row in self.movies.iterrows():
                if predicted[0] == i:
                    print('\nSince you\'ve liked', self.watched_movie, 'We recommend: ', row['title'], 'genres:', row['genres'])
                if i == 999:
                    print(self.watched_movie, 'movie\'s genre:', row['genres'])
                    print()

        else:
            print('Something went wrong with the analysis')

    def get_highest(self, recommended):
        """
        Gets the highest percantage that matches the movie
        :param recommended: recommended movies
        :return: returns the top movie
        """
        return sorted(recommended, key=lambda elem: elem[1], reverse=True)[1]

    def merge(self, row):
        """
        Merges features from a single row
        :param row:
        :return:
        """
        return '{} {} {} {}'.format(row['title'], row['genres'], row['actorName'], row['directorName'])

    def read_table(self, query):
        """
        Executes MySQL queries and converts it into pandas format
        :param query: mysql query that needs to be executed
        :return: result of the query in pandas format
        """
        if self.connection.is_connected():
            return pd.read_sql_query(query, self.connection)


def main():
    mysql = MySQL()
    content_based = ContentBasedRecommendationSystem('Iron Man (1931)', mysql)
    content_based.train()


if __name__ == '__main__':
    main()
