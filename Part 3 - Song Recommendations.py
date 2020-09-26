# Song Recommendations

# Imports
# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import functions as F


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Compute suitable number of partitions

conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

# Q1

# (a)

# load the mismatches data

mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2019/DATA420-19S2/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 40)

print(mismatches.count())  # 19094

# load the manually accepted data

with open("/scratch-network/courses/2019/DATA420-19S2/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 40)

print(matches_manually_accepted.count())  # 488

# load the Taste Profile dataset

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
triplets.cache()
triplets.show(10, 50)

# remove the mismatches data which is in manually accepted data

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti") 

# remove the mismatches song from Taste Profile dataset

triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

print(mismatches_not_accepted.count())  # 19093
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111

# the number of unique users

unique_user_counts = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.count(col("song_id")).alias("song_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
unique_user_counts.cache()
unique_user_counts.count() # 1019318

# the number of unique songs

unique_song_counts = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.count(col("user_id")).alias("user_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)

unique_song_counts.cache()
unique_song_counts.count() # 378310

unique_user_counts.show(10, False)

# +----------------------------------------+----------+----------+
# |user_id                                 |song_count|play_count|
# +----------------------------------------+----------+----------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195       |13074     |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362      |9104      |
# |3fa44653315697f42410a30cb766a4eb102080bb|146       |8025      |
# |a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|518       |6506      |
# |d7d2d888ae04d16e994d6964214a1de81392ee04|1257      |6190      |
# |4ae01afa8f2430ea0704d502bc7b57fb52164882|453       |6153      |
# |b7c24f770be6b802805ac0e2106624a517643c17|1364      |5827      |
# |113255a012b2affeab62607563d03fbdf31b08e7|1096      |5471      |
# |99ac3d883681e21ea68071019dba828ce76fe94d|939       |5385      |
# |6d625c6557df84b60d90426c0116138b617b9449|1307      |5362      |
# +----------------------------------------+----------+----------+

# (b)

# check the different songs has the most user played

statistics = (
    unique_user_counts
    .select("song_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#               count                mean              stddev min    max
# song_count  1019318   44.92720721109605   54.91113199747355   3   4316
# play_count  1019318  128.82423149596102  175.43956510304616   3  13074

# the number of different songs has the most active user played: 4316

# the percentage of the total number of unique songs in the dataset

4316/unique_song_counts.count() #0.011408633131558774

# (c)

# caculate the song popularity:  1.the number of times it has been played 2.the number of users have played

song_popularity = triplets_not_mismatched.groupBy('song_id') \
    .agg(F.count('user_id').alias('user_count'), 
        F.sum('plays').alias('sum_play_count')) \
    .orderBy('user_count', ascending = False)
    
song_popularity.show()
song_popularity.count() #378310

#+------------------+----------+--------------+
#|           song_id|user_count|sum_play_count|
#+------------------+----------+--------------+
#|SOAXGDH12A8C13F8A1|     90444|        356533|
#|SOBONKR12A58A7A7E0|     84000|        726885|
#|SOSXLTC12AF72A7F54|     80656|        527893|
#|SONYKOW12AB01849C9|     78353|        292642|
#|SOEGIYH12A6D4FC0E3|     69487|        389880|
#|SOLFXKT12AB017E3E0|     64229|        197181|
#|SOFLJQZ12A6D4FADA6|     58610|        185653|
#|SOUSMXX12AB0185C24|     53260|        155529|
#|SOWCKVR12A8C142411|     52080|        145725|
#|SOUVTSM12AC468F6A7|     51022|        155717|
#|SOTWNDJ12A8C143984|     47011|        174080|
#|SOPUCYA12A8C13A694|     46078|        274627|
#|SOHTKMO12AB01843B0|     46077|        236494|
#|SOKLRPJ12A8C13C3FE|     45495|        128837|
#|SOCVTLJ12A6310F0FD|     42535|        114362|
#|SOPPROJ12AB0184E18|     41811|        124162|
#|SOBOUPA12A6D4F81F1|     41093|        225652|
#|SOPTLQL12AB018D56F|     40497|        122318|
#|SOOFYTN12A6D4F9B35|     40403|        241669|
#|SOBOAFP12A8C131F36|     39851|        127044|
#+------------------+----------+--------------+
#only showing top 20 rows


# write csv for plot

#outputpath = 'hdfs:///user/ylu60/output/MSD/'
#song_popularity.repartition(1).write.format('csv').save(outputpath+'song_popularity.csv')

# copy the output from HDFS

#!hdfs dfs -copyToLocal  hdfs:///user/ylu60/output/MSD/song_popularity.csv

# descriptive statistics for the song_popularity

statistics = (
    song_popularity
    .select("user_count", "sum_play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#                 count                mean              stddev min     max
#user_count      378310  121.05181200602681   748.6489783736907   1   90444
#sum_play_count  378310   347.1038513388491  2978.6053488382104   1  726885

# caculate the quantile

# 1.the number of times it has been played

song_popularity.approxQuantile("sum_play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05) #[1.0, 8.0, 44.0, 301.0, 726885.0]

# 2.the number of users have played

song_popularity.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05) #[1.0, 4.0, 16.0, 42.0, 90444.0]

# (d)

# make the threshold for the clean dataset

# make the threshold for the number of songs which users have listen to: M

unique_user_counts.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# [3.0, 16.0, 27.0, 59.0, 4316.0] # M should be 15

# make the threshold for the number of times which a song have been played: N

unique_song_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# [1.0, 9.0, 29.0, 154.0, 726885.0] # N should be 10

M = 15 # number of songs which users have listen to threshold
N = 10  # number of times which a song have been played threshold

# filter the active users

active_users = unique_user_counts.filter(unique_user_counts.song_count > M)

# number of users

active_users.count() # 744910

# proportion of all users

active_users.count() / unique_user_counts.count() # 0.7307925495282139

# filter the active songs

popular_songs = unique_song_counts.filter(unique_song_counts.play_count > N)

# number of users

popular_songs.count() # 266527

# proportion of all users

popular_songs.count() / unique_song_counts.count() # 0.7045201025613914

# create a clean dataset

triplets_cleaned = triplets_not_mismatched \
    .join(active_users, on='user_id', how = "inner") \
    .join(popular_songs, on='song_id', how = "inner") \
    .drop('song_count', 'play_count', 'user_count', 'play_count')
triplets_cleaned.cache()
triplets_cleaned.show()

#+------------------+--------------------+-----+
#|           song_id|             user_id|plays|
#+------------------+--------------------+-----+
#|SOGEWRX12AB0189432|00007ed2509128dcd...|    2|
#|SOJUDEO12A8C13F8E5|00007ed2509128dcd...|    1|
#|SODGBAO12A8C13F8C1|00007ed2509128dcd...|    1|
#|SODESWY12AB0182F2E|00007ed2509128dcd...|    1|
#|SOIITTN12A6D4FD74D|00007ed2509128dcd...|    1|
#|SORREOD12AB0189427|00007ed2509128dcd...|    1|
#|SOPFVLV12AB0185C5D|00007ed2509128dcd...|    2|
#|SOAYETG12A67ADA751|00007ed2509128dcd...|    2|
#|SOKUCDV12AB0185C45|00007ed2509128dcd...|    4|
#|SOROLRE12A8C13943A|00007ed2509128dcd...|    1|
#|SOTOAAN12AB0185C68|00007ed2509128dcd...|    3|
#|SORPSOF12AB0188C39|00007ed2509128dcd...|    2|
#|SOICJAD12A8C13B2F4|00007ed2509128dcd...|    1|
#|SOCXHEU12A6D4FB331|00007ed2509128dcd...|    1|
#|SOMUZHL12A8C130AFE|00007ed2509128dcd...|    1|
#|SOHYYDE12AB018A608|00007ed2509128dcd...|    1|
#|SOULNCS12A8C13C163|00007ed2509128dcd...|    1|
#|SOUOCBA12A6D4FAAFF|00007ed2509128dcd...|    1|
#|SOLJSMV12A8C13B2D9|00007ed2509128dcd...|    1|
#|SOHBYIJ12A6D4FB344|00007ed2509128dcd...|    1|
#+------------------+--------------------+-----+
#only showing top 20 rows

# number of clean dataset

triplets_cleaned.count() # 42223389

# predictions of all dataset

triplets_cleaned.count() / triplets_not_mismatched.count()  # 0.9220064779404072

# (e)

# Imports

from pyspark.ml.feature import StringIndexer

# Encoding
# create integer index for user and song

user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_encoded")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="song_id_encoded")

user_id_indexer_model = user_id_indexer.fit(triplets_cleaned)
song_id_indexer_model = song_id_indexer.fit(triplets_cleaned)

triplets_cleaned = user_id_indexer_model.transform(triplets_cleaned)
triplets_cleaned = song_id_indexer_model.transform(triplets_cleaned)

# split dataset to training(70%) and test data(30%)

# Imports

from pyspark.sql.window import *

# Splits

training, test = triplets_cleaned.randomSplit([0.7, 0.3])

# check if some data in test dataset do not have records in training dataset

test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}") # check

#training:          29551317
#test:              12672072
#test_not_training: 0

# Q2 

# (a)

# Imports

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer

from pyspark.mllib.evaluation import RankingMetrics

# create ALS model

als = ALS(maxIter=5, regParam=0.01, userCol="user_id_encoded", itemCol="song_id_encoded", ratingCol="plays")
als_model = als.fit(training)
predictions = als_model.transform(test)

predictions = predictions.orderBy(col("user_id"), col("song_id"), col("prediction").desc())
predictions.cache()

predictions.show(50, False)

#+------------------+----------------------------------------+-----+---------------+---------------+-----------+
#|song_id           |user_id                                 |plays|user_id_encoded|song_id_encoded|prediction |
#+------------------+----------------------------------------+-----+---------------+---------------+-----------+
#|SOBDRND12A8C13FD08|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688110.0       |2643.0         |1.4357135  |
#|SODRFRJ12A8C144167|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688110.0       |1103.0         |2.0874386  |
#|SOJAMXH12A8C138D9B|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688110.0       |1771.0         |0.72493017 |
#|SOLHJTO12A81C21354|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688110.0       |28329.0        |1.5677953  |
#|SOMMIUM12A8C13FD49|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688110.0       |4117.0         |0.5955186  |
#|SOPEVJE12A67ADE837|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688110.0       |6490.0         |0.39681074 |
#|SOUPXLX12A67ADE83A|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688110.0       |11522.0        |0.8927409  |
#|SOVYIYI12A8C138D88|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688110.0       |1121.0         |0.9268975  |
#|SOWUCFL12AB0188263|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688110.0       |8784.0         |1.5310048  |
#|SOCJCVE12A8C13CDDB|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |394.0          |1.5223101  |
#|SODEYDM12A58A77072|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |228.0          |1.4352893  |
#|SOGNFOD12A6D4F8584|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |5728.0         |0.96178544 |
#|SOJERWB12A8C13E654|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |713.0          |2.0717106  |
#|SOMNPAP12A8C1385D6|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |7257.0         |0.89477617 |
#|SOPOFBW12AB0187196|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |1833.0         |1.196528   |
#|SOQLDTI12AB018C80A|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553259.0       |2708.0         |0.77252245 |
#|SOIXKRK12A8C140BD1|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325291.0       |886.0          |1.0228877  |
#|SOJGSIO12A8C141DBF|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325291.0       |324.0          |1.9415383  |
#|SOOLKLP12AF729D959|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|2    |325291.0       |1137.0         |1.1392617  |
#|SOPWKOX12A8C139D43|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|4    |325291.0       |1941.0         |1.4216511  |
#|SOQFXDQ12AF72AD0EE|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325291.0       |1982.0         |1.3379695  |
#|SOUGLUN12A8C14282A|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325291.0       |4668.0         |2.0324798  |
#|SOWNIUS12A8C142815|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|2    |325291.0       |1061.0         |1.3159144  |
#|SOXLKNJ12A58A7E09A|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|3    |325291.0       |755.0          |1.1245317  |
#|SOYDTRQ12AF72A3D61|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|2    |325291.0       |262.0          |1.7288008  |
#|SOZVCRW12A67ADA0B7|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325291.0       |35.0           |1.5911548  |
#|SOAAVNU12A6D4F65BA|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |11477.0        |-2.017869  |
#|SOBPQSZ12A6D4FACBB|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |3496.0         |-0.5746574 |
#|SOCGHCY12A58A7C997|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |14412.0        |1.9394704  |
#|SOHLBZD12A58A7B0AC|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |5237.0         |0.9838602  |
#|SOILWTV12A6D4F4A4B|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |6237.0         |1.3963485  |
#|SOKBRAQ12A6D4F8FEC|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|7    |416001.0       |3128.0         |5.027764   |
#|SOKXYIL12AB0189157|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|2    |416001.0       |6943.0         |1.2064401  |
#|SONPCAF12A81C21DE9|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |11075.0        |-8.428901  |
#|SOVSLCI12A8C137ED3|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416001.0       |9075.0         |0.19506767 |
#|SOABUZM12A6D4FB8C9|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |28095.0        |0.7617986  |
#|SOANVMB12AB017F1DD|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |87507.0        |0.6468988  |
#|SOAYETG12A67ADA751|00007ed2509128dcdd74ea3aac2363e24e9dc06b|2    |229429.0       |568.0          |1.2400279  |
#|SOBWGGV12A6D4FD72E|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |24172.0        |0.6652723  |
#|SOCRKNT12AB018940D|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |54977.0        |0.8478452  |
#|SOCXHEU12A6D4FB331|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |69489.0        |-0.75531244|
#|SODGBAO12A8C13F8C1|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |63361.0        |0.702512   |
#|SOEPNVO12AF72A7CC9|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |29496.0        |0.6787727  |
#|SOHYYDE12AB018A608|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |50334.0        |6.6693053  |
#|SOIZRWS12AF72A1163|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |44707.0        |1.0408559  |
#|SOOGCBL12A8C13FA4E|00007ed2509128dcdd74ea3aac2363e24e9dc06b|2    |229429.0       |64089.0        |1.3603142  |
#|SOOKJWB12A6D4FD4F8|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |27068.0        |1.5054269  |
#|SORKZYO12AF72A8CA2|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |9049.0         |-0.12826775|
#|SOROLRE12A8C13943A|00007ed2509128dcdd74ea3aac2363e24e9dc06b|1    |229429.0       |50046.0        |0.7922084  |
#|SOTOAAN12AB0185C68|00007ed2509128dcdd74ea3aac2363e24e9dc06b|3    |229429.0       |30155.0        |2.0566928  |
#+------------------+----------------------------------------+-----+---------------+---------------+-----------+
#only showing top 50 rows

# (b)

# generate the recommendations by using ALS model

N = 10 #number of item to recommend
windowSpec = Window.partitionBy('user_id_encoded').orderBy(F.col('prediction').desc())

song_recommendation = predictions \
    .select('user_id_encoded', 'song_id_encoded', 'prediction', F.rank().over(windowSpec).alias('rank')) \
    .where(F.col('rank')<=N) \
    .groupBy('user_id_encoded') \
    .agg(F.collect_list('song_id_encoded').alias('song_recommended'))

song_recommendation.cache()
song_recommendation.show(5)

#+---------------+--------------------+
#|user_id_encoded|    song_recommended|
#+---------------+--------------------+
#|            6.0|[139983.0, 190655...|
#|           12.0|[49312.0, 98230.0...|
#           13.0|[95962.0, 215931....|
#|           58.0|[21954.0, 211741....|
#|           79.0|[26517.0, 35570.0...|
#+---------------+--------------------+
#only showing top 5 rows

# from test dataset, collect all the songs which are relevant to users
# into a array in a single columns, begin with the one with highest rating

windowSpec = Window.partitionBy('user_id_encoded').orderBy(F.col('plays').desc())

song_relevant = test \
    .select('user_id_encoded', 'plays', 'song_id_encoded', F.rank().over(windowSpec).alias('rank')) \
    .where(F.col('rank') <=N ) \
    .groupBy('user_id_encoded') \
    .agg(F.collect_list('song_id_encoded').alias('song_relevant'))

song_relevant.cache()
song_relevant.show(5, False)

#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#|user_id_encoded|song_relevant                                                                                                                                             |
#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#|6.0            |[965.0, 1056.0, 354.0, 26356.0, 323.0, 5035.0, 5097.0, 1119.0, 765.0, 9289.0]                                                                             |
#|12.0           |[20442.0, 1805.0, 695.0, 59936.0, 190764.0, 196815.0, 43372.0, 210888.0, 63265.0, 75059.0]                                                                |
#|13.0           |[23526.0, 220351.0, 160987.0, 116875.0, 21744.0, 2511.0, 21857.0, 7393.0, 10770.0, 2813.0, 261439.0, 5553.0, 37001.0, 49789.0, 232741.0, 47100.0, 64449.0]|
#|58.0           |[466.0, 22509.0, 24261.0, 23393.0, 9973.0, 10123.0, 35177.0, 4875.0, 7538.0, 4081.0, 18748.0]                                                             |
#|79.0           |[78067.0, 269.0, 154151.0, 73917.0, 5763.0, 167641.0, 1001.0, 935.0, 175793.0, 112396.0]                                                                  |
#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#only showing top 5 rows

recommendation_and_relevant = song_recommendation.join(song_relevant, on='user_id_encoded') 
recommendation_and_relevant.cache()
recommendation_and_relevant.show(5, False)

#+---------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#|user_id_encoded|song_recommended                                                                                  |song_relevant                                                                                                                                             |
#+---------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#|6.0            |[139983.0, 190655.0, 10741.0, 229553.0, 7323.0, 24253.0, 21433.0, 28906.0, 32027.0, 29716.0]      |[965.0, 1056.0, 354.0, 26356.0, 323.0, 5035.0, 5097.0, 1119.0, 765.0, 9289.0]                                                                             |
#|12.0           |[49312.0, 98230.0, 40347.0, 84354.0, 10950.0, 196815.0, 45347.0, 92914.0, 190764.0, 9553.0]       |[20442.0, 1805.0, 695.0, 59936.0, 190764.0, 196815.0, 43372.0, 210888.0, 63265.0, 75059.0]                                                                |
#|13.0           |[95962.0, 215931.0, 213959.0, 73313.0, 195389.0, 155897.0, 118541.0, 161202.0, 121831.0, 168253.0]|[23526.0, 220351.0, 160987.0, 116875.0, 21744.0, 2511.0, 21857.0, 7393.0, 10770.0, 2813.0, 261439.0, 5553.0, 37001.0, 49789.0, 232741.0, 47100.0, 64449.0]|
#|58.0           |[21954.0, 211741.0, 5815.0, 12630.0, 42079.0, 28053.0, 11502.0, 2991.0, 18397.0, 10015.0]         |[466.0, 22509.0, 24261.0, 23393.0, 9973.0, 10123.0, 35177.0, 4875.0, 7538.0, 4081.0, 18748.0]                                                             |
#|79.0           |[26517.0, 35570.0, 221941.0, 74911.0, 60883.0, 78326.0, 51091.0, 49356.0, 159549.0, 49769.0]      |[78067.0, 269.0, 154151.0, 73917.0, 5763.0, 167641.0, 1001.0, 935.0, 175793.0, 112396.0]                                                                  |
#+---------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+
#only showing top 5 rows

from pyspark.mllib.evaluation import RankingMetrics

# (c)
# calculate ranking metricss
metrics = RankingMetrics(
    recommendation_and_relevant \
    .select(['song_recommended', 'song_relevant']) \
    .rdd \
    .map(lambda row: (row[0], row[1]))
    )


print("PRECISION @ 10: ", metrics.precisionAt(N))
print("MAP @ 10: ", metrics.meanAveragePrecision)
print("NDCG @10: ", metrics.ndcgAt(N))

#PRECISION @ 10:  0.7214197272224288
#MAP @ 10:  0.727511046690355
#NDCG @10:  0.8838562387037185

K = 5 
print("PRECISION @ 5: ", metrics.precisionAt(K))
print("MAP @ 5: ", metrics.meanAveragePrecision)
print("NDCG @5: ", metrics.ndcgAt(K))

#PRECISION @ 5:  0.8564546973286272
#MAP @ 5:  0.727511046690355
#NDCG @5:  0.8868546173633727


