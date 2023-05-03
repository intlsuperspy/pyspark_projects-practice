import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt

spark = SparkSession\
        .builder \
        .appName("Spark Project") \
        .getOrCreate()

#create a function to load in the dataframe

def load_dataframe(filename,extension):
    df = spark.read.format(extension).options(header = 'true').load(filename)
    return df

#creating the dataframe
df_matches = load_dataframe("Documents/Spark/Matches.csv", "csv")
df_matches.show()


#renaming columns
df_matches = df_matches.withColumnRenamed("FTHG", "HomeTeamGoals")

#another way / this way is using zip and list comprenhension 
'''
it unpacks the resulting list of pairs into individual arguments. 
This allows each pair to be treated as a separate argument to the old_new_cols list constructor, which creates a new list containing all of the pairs.
'''
old_column = df_matches.columns[-2:]
new_column= [ "AwayTeamGoals", "FinalResult"]

old_new_cols = [*zip(old_column, new_column)]

for old_column, new_column in old_new_cols:
    df_matches = df_matches.withColumnRenamed(old_column, new_column)


'''
Q: Who are the winners of the D1 division in the Germany Football Association (Bundesliga) between 2000–2010?
'''

df_matches = df_matches \
    .withColumn('HomeTeamWin', when(col('FinalResult') == 'H', 1).otherwise(0)) \
    .withColumn('AwayTeamWin', when(col('FinalResult') == 'A', 1).otherwise(0)) \
    .withColumn('GameTie', when(col('FinalResult') == 'D', 1).otherwise(0))

df_matches.dtypes

df_winners_d1 = df_matches.filter((df_matches.Div == "D1") & (df_matches.Season.between(2000, 2010)))

home_game_wins = df_winners_d1.groupBy("Season", "HomeTeam") \
                .agg(sum('HomeTeamWin').alias('TotalHomeWin'),
                        sum('AwayTeamWin').alias('TotalHomeLoss'),
                        sum('GameTie').alias('TotalHomeTie'),
                        sum('HomeTeamGoals').alias('HomeScoredGoals'),
                        sum('AwayTeamGoals').alias('HomeAgainstGoals')) \
                .withColumnRenamed('HomeTeam', 'Team')

home_game_wins.show()

away_game_wins =  df_winners_d1.groupby('Season', 'AwayTeam') \
       .agg(sum('AwayTeamWin').alias('TotalAwayWin'),
            sum('HomeTeamWin').alias('TotalAwayLoss'),
            sum('GameTie').alias('TotalAwayTie'),
            sum('AwayTeamGoals').alias('AwayScoredGoals'),
            sum('HomeTeamGoals').alias('AwayAgainstGoals'))  \
       .withColumnRenamed('AwayTeam', 'Team')

away_game_wins.show()