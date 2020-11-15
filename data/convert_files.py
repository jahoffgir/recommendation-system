import pandas as pd

"""
This code merges all the rows of tags per user per move as one row 
for convenience while loading in database

__author__: "Jahongir Amirkulov"
__author__: "Nishi Parameshwara"
__author__: "Sharwari Salunkhe"

"""

df = pd.read_csv('tags.csv', index_col=None)
# doesnt happen in place but string are needed for typesetting, can be parsed later
df = df.astype({'tag': str, 'timestamp': str})
# aggregating tags and time stamp to make userId and movieId primary keys
df = df.groupby(['userId', 'movieId']).agg({'tag': '|'.join, 'timestamp': '|'.join})
df.to_csv('ref_tags.csv')
