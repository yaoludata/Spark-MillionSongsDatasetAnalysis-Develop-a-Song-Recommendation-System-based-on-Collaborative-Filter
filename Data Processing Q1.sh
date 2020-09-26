# DATA420-19S2 Assignment 2

# Data Processing
# Q1
# (a)
# Determine how the data is structured 
hdfs dfs -ls -R hdfs:///data/msd/

# check the size of data
hdfs dfs -ls hdfs:///data/msd/
#drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:10 hdfs:///data/msd/audio
#drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:17 hdfs:///data/msd/genre
#drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:21 hdfs:///data/msd/main
#drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:23 hdfs:///data/msd/tasteprofile

hdfs dfs -du -s -h hdfs:///data/msd/
#12.9 G  103.5 G  hdfs:///data/msd

hdfs dfs -du -h hdfs:///data/msd/
#12.3 G   98.1 G   hdfs:///data/msd/audio
#30.1 M   241.0 M  hdfs:///data/msd/genre
#174.4 M  1.4 G    hdfs:///data/msd/main
#490.4 M  3.8 G    hdfs:///data/msd/tasteprofile

# (c)
# count the number of rows in each datasets

# audio/attributes

hdfs dfs -cat hdfs:///data/msd/audio/attributes/* | wc -l #3929

# audio/features 

hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/* | gunzip | wc -l #994623
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/* | gunzip | wc -l #995001
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-mvd-v1.0.csv/* | gunzip | wc -l #994188
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-rh-v1.0.csv/* | gunzip | wc -l #994188
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-rp-v1.0.csv/* | gunzip | wc -l #994188
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-ssd-v1.0.csv/* | gunzip | wc -l #994188
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-trh-v1.0.csv/* | gunzip | wc -l #994188
hdfs dfs -cat hdfs:///data/msd/audio/features/msd-tssd-v1.0.csv/* | gunzip | wc -l #994188

# audio/statisics 

hdfs dfs -cat hdfs:///data/msd/audio/statistics/sample_properties.csv.gz | gunzip | wc -l #992866

#audio:49963215

# genre

hdfs dfs -cat hdfs:///data/msd/genre/* | wc -l #1103077

hdfs dfs -cat hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv | wc -l #422714
hdfs dfs -cat hdfs:///data/msd/genre/msd-MASD-styleAssignment.tsv | wc -l #273936
hdfs dfs -cat hdfs:///data/msd/genre/msd-topMAGD-genreAssignment.tsv | wc -l #406427

# main

hdfs dfs -cat hdfs:///data/msd/main/summary/* | gunzip | wc -l #2000002

# main/analysis
!hdfs dfs -cat hdfs:///data/msd/main/summary/analysis.csv.gz | gunzip | wc -l #1000001
# main/metadata
!hdfs dfs -cat hdfs:///data/msd/main/summary/metadata.csv.gz | gunzip | wc -l #1000001

# tasteprofile/mismatches

hdfs dfs -cat hdfs:///data/msd/tasteprofile/mismatches/* | wc -l #20032

# tasteprofile/triplets.tsv

hdfs dfs -cat hdfs:///data/msd/tasteprofile/triplets.tsv/* | gunzip | wc -l #48373586



 


















