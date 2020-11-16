import csv
import pandas as pd

from my_sql import MySQL
from lenskit.algorithms import Recommender
from lenskit.algorithms.user_knn import UserUser

"""
User-User Collaborative Filtering for movie recommendation system

This is primarily used to recommend a movie for mulitple users given their preferences.

@author Jahongir Amirkulov
@author Nishi Parameshwara
@author Sharwari Salunkhe
"""


class UserUserCollaborativeSystem:
    def __init__(self, mysql):
        self.mysql = mysql
        self.connection = mysql.get_connection()
        self.movies = self.read_table("""select * from movielenstable WHERE title IS NOT NULL AND genres IS NOT NULL;""")
        self.movies.columns = ['item', 'title', 'genres']
        self.ratings = self.read_table("""select * from lensratings WHERE rating IS NOT NULL;""")
        self.ratings.columns = ['user', 'item', 'rating']
        self.user_user = UserUser(15, min_nbrs=3)
        self.algorithm = Recommender.adapt(self.user_user)
        self.algorithm.fit(self.ratings)

    def __del__(self):
        print("Closing db connection")
        self.connection.close()

    def train(self, pref_one, pref_two, num_movies_rec=10):
        """
        Trains the UserUser model based on the two preferences that people rated
        :param pref_one: rating of movies for person one
        :param pref_two: rating of movies for person two
        :param num_movies_rec: number of movies to recommend
        :return: returns the recommended movies
        """
        # TODO Refactor
        merged = {}
        for k in pref_one:
            if k in pref_two:
                merged.update({k: float((pref_one[k]+pref_two[k])/2)})
            else:
                merged.update({k: pref_one[k]})
        for k in pref_two:
            if k not in merged:
                merged.update({k: pref_two[k]})

        recommendation = self.algorithm.recommend(-1, 50, ratings=pd.Series(merged))
        result = recommendation.join(self.movies['genres'], on='item')
        result = result.join(self.movies['title'], on='item')
        result = result[result.columns[2:]].dropna()

        recommended_movies = []
        for i in range(len(result.head(num_movies_rec))):
            recommended_movies.append((result.iloc[i, 1], result.iloc[i, 0]))

        print(recommended_movies)

    def read_table(self, query):
        """
        Executes MySQL queries and converts it into pandas format
        :param query: mysql query that needs to be executed
        :return: result of the query in pandas format
        """
        if self.connection.is_connected():
            return pd.read_sql_query(query, self.connection)

    def generic_movies(self, votes=20):
        """
        Gives generic movies based on other user ratings
        :return: generic movies
        """
        ratings_mean = self.ratings.groupby(['item']).mean()
        ratings_count = self.ratings.groupby(['item']).count()
        ratings_mean = ratings_mean.loc[ratings_count['rating'] > votes]
        ratings_mean = ratings_mean.sort_values(by="rating", ascending=False)
        merged = ratings_mean.join(self.movies['genres'], on='item')
        merged = merged.join(self.movies['title'], on='item')
        return merged[merged.columns[3:]].dropna()


def main():
    mysql = MySQL()
    user_user = UserUserCollaborativeSystem(mysql)

    person_one, person_two = {}, {}

    # TODO Refactor
    with open("data/user_preferences/person1.csv", newline='') as csvfile:
        ratings_reader = csv.DictReader(csvfile)
        for row in ratings_reader:
            if ((row['ratings'] != "") and (float(row['ratings']) > 0) and (float(row['ratings']) < 6)):
                person_one.update({int(row['item']): float(row['ratings'])})

    with open("data/user_preferences/person2.csv", newline='') as csvfile:
        ratings_reader = csv.DictReader(csvfile)
        for row in ratings_reader:
            if ((row['ratings'] != "") and (float(row['ratings']) > 0) and (float(row['ratings']) < 6)):
                person_two.update({int(row['item']): float(row['ratings'])})

    user_user.train(person_one, person_two)


if __name__ == '__main__':
    main()
