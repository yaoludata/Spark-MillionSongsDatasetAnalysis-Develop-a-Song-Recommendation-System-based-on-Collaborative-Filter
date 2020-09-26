
# Audio Similarity

# Q1

#(a)
# load the dataset

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

audio_sample = (
    spark.read.format("com.databricks.spark.csv")
    .load("hdfs:///data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/*.*")
    )
    
for col in audio_sample.columns[:-1]:
    audio_sample = (
        audio_sample
        .withColumn(col, F.col(col).cast(DoubleType()))
        )

audio_sample.printSchema()

# chang the last column name to 'track_id'
# remove the '' from the track_id column

audio_feature_lists = [audio_sample]

audio_feature_list = []
for audio_feature_df in audio_feature_lists:
    audio_feature_df = audio_feature_df.withColumnRenamed(audio_feature_df.columns[-1], 'track_id')
    track_id = audio_feature_df.select('track_id').rdd.map(lambda x: x[0]).take(1)[0]
    if track_id.startswith("'"):
        audio_feature_df = audio_feature_df.withColumn(
            'track_id', F.substring(F.col('track_id'), 2, 18)
            )
    audio_feature_list.append(audio_feature_df) 

# count the number of columns and rows for the dataset

for audio_feature_df in audio_feature_list:
    print(len(audio_feature_df.columns), audio_feature_df.count()) #11 994623

# Produce descriptive statistics for each feature column
    
statistics = (
    audio_feature_df
    .drop("track_id")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#      count                 mean               stddev        min       max
#_c0  994623  0.15498176001746342  0.06646213086143025        0.0     0.959
#_c1  994623   10.384550576952307    3.868001393874684        0.0     55.42
#_c2  994623    526.8139724398096    180.4377549977526        0.0    2919.0
#_c3  994623    35071.97543290272   12806.816272955562        0.0  407100.0
#_c4  994623    5297870.369577217   2089356.4364558063        0.0   4.657E7
#_c5  994623   0.3508444432531317  0.18557956834383815        0.0     2.647
#_c6  994623   27.463867987840715    8.352648595163766        0.0     117.0
#_c7  994623   1495.8091812075545    505.8937639190231        0.0    5834.0
#_c8  994623   143165.46163257837   50494.276171032274  -146300.0  452500.0
#_c9  994623  2.396783048473542E7    9307340.299219666        0.0   9.477E7

# are there any features strongly correlated
# column, row = features
# value = correlation coefficient

from pyspark.mllib.stat import Statistics
import pandas as pd

audio_feature = (
    audio_feature_df
    .drop("track_id")
    .rdd.map(lambda row: row[0:]))
    
correlation = Statistics.corr(audio_feature, method="pearson")
correlation_df = pd.DataFrame(correlation)
correlation_df

#          0         1         2         3         4         5         6         7         8         9
#0  1.000000  0.426280  0.296306  0.061039 -0.055336  0.754208  0.497929  0.447565  0.167466  0.100407
#1  0.426280  1.000000  0.857549  0.609521  0.433797  0.025228  0.406923  0.396354  0.015607 -0.040902
#2  0.296306  0.857549  1.000000  0.803010  0.682909 -0.082415  0.125910  0.184962 -0.088174 -0.135056
#3  0.061039  0.609521  0.803010  1.000000  0.942244 -0.327691 -0.223220 -0.158231 -0.245034 -0.220873
#4 -0.055336  0.433797  0.682909  0.942244  1.000000 -0.392551 -0.355019 -0.285966 -0.260198 -0.211813
#5  0.754208  0.025228 -0.082415 -0.327691 -0.392551  1.000000  0.549015  0.518503  0.347112  0.278513
#6  0.497929  0.406923  0.125910 -0.223220 -0.355019  0.549015  1.000000  0.903367  0.516499  0.422549
#7  0.447565  0.396354  0.184962 -0.158231 -0.285966  0.518503  0.903367  1.000000  0.772807  0.685645
#8  0.167466  0.015607 -0.088174 -0.245034 -0.260198  0.347112  0.516499  0.772807  1.000000  0.984867
#9  0.100407 -0.040902 -0.135056 -0.220873 -0.211813  0.278513  0.422549  0.685645  0.984867  1.000000

# _c3 and _c4, _c6 and _c7, _c8 and _c9 are strongly correlated (correlation coefficient > 0.90)

# (b)

#from pyspark.sql.functions import rand, randn

# Load the MSD ALL Music Genre Dataset(MAGD)

MSD_genre_schema = StructType([
    StructField('track_id', StringType()),
    StructField('genre', StringType()),
    ])

MSD_genre = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", "\t")
    .schema(MSD_genre_schema)
    .load("/data/msd/genre/msd-MAGD-genreAssignment.tsv")
    )
    
MSD_genre.show(5, False)

#+------------------+--------+
#|track_id          |genre   |
#+------------------+--------+
#|TRAAAAK128F9318786|Pop_Rock|
#|TRAAAAV128F421A322|Pop_Rock|
#|TRAAAAW128F429D538|Rap     |
#|TRAAABD128F429CF47|Pop_Rock|
#|TRAAACV128F423E09E|Pop_Rock|
#+------------------+--------+

# create an output directory

#!hdfs dfs -mkdir hdfs:///user/ylu60/output/MSD/

# write csv for plot

#outputpath = 'hdfs:///user/ylu60/output/MSD/'
#MSD_genre.repartition(1).write.format('csv').save(outputpath+'MSD_genre.csv')

# copy the output from HDFS

#!hdfs dfs -copyToLocal  hdfs:///user/ylu60/output/MSD/MSD_genre.csv

# (c)

# merge the genres dataset and the audio features dataset

audio_feature_genre = audio_feature_df.join(MSD_genre, on='track_id')
audio_feature_genre.show(5, False)

# count the number of row in audio_feature_genre dataset

audio_feature_genre.count() #420620

#+------------------+------+-----+-----+-------+------+-----+--------+----------+
#|track_id          |_c0   |_c1  |_c2  |_c3    |_c5   |_c6  |_c8     |genre     |
#+------------------+------+-----+-----+-------+------+-----+--------+----------+
#|TRAAABD128F429CF47|0.1308|9.587|459.9|27280.0|0.2474|26.02|67790.0 |Pop_Rock  |
#|TRAABPK128F424CFDB|0.1208|6.738|215.1|11890.0|0.4882|41.76|220400.0|Pop_Rock  |
#|TRAACER128F4290F96|0.2838|8.995|429.5|31990.0|0.5388|28.29|185100.0|Pop_Rock  |
#|TRAADYB128F92D7E73|0.1346|7.321|499.6|38460.0|0.2839|15.75|116500.0|Jazz      |
#|TRAAGHM128EF35CF8E|0.1563|9.959|502.8|26190.0|0.3835|28.24|180800.0|Electronic|
#+------------------+------+-----+-----+-------+------+-----+--------+----------+
#only showing top 5 rows



# Q2

# (b)

# convert the genre column into a column representing if the song is 'Electronic' or some other genre as a binary label

binary_audio_feature_genre = (audio_feature_genre
    .withColumn('label', F.when(F.col('genre') == 'Electronic', 1).otherwise(0))
    .drop('track_id')
    )

# Class balance

(
    binary_audio_feature_genre
    .groupBy("label")
    .count()
    .show()
)

#+-----+------+
#|label| count|
#+-----+------+
#|    1| 40666|
#|    0|379954|
#+-----+------+

# (c)

# split the dataset into traning and teset sets

# randomSplit

training, test = binary_audio_feature_genre.randomSplit([0.8, 0.2], seed = 1000)
training.groupBy('label').count().show()
test.groupBy('label').count().show()

# training
#+-----+------+
#|label| count|
#+-----+------+
#|    1| 32631|
#|    0|304385|
#+-----+------+

# test
#+-----+-----+
#|label|count|
#+-----+-----+
#|    1| 8035|
#|    0|75569|
#+-----+-----+

# subsampling

training = training.sampleBy("label", fractions={0: 0.1, 1: 1}, seed=1000)
training.groupBy('label').count().show()
test.groupBy('label').count().show()

# (d)

# check the variables

binary_audio_feature_genre.printSchema()

#root
# |-- _c0: double (nullable = true)
# |-- _c1: double (nullable = true)
# |-- _c2: double (nullable = true)
# |-- _c3: double (nullable = true)
# |-- _c4: double (nullable = true)
# |-- _c5: double (nullable = true)
# |-- _c6: double (nullable = true)
# |-- _c7: double (nullable = true)
# |-- _c8: double (nullable = true)
# |-- _c9: double (nullable = true)
# |-- genre: string (nullable = true)
# |-- label: integer (nullable = false)


# Preparing Data for Machine Learning

from pyspark.ml.feature import VectorAssembler, PCA, StringIndexer, StandardScaler
from pyspark.ml import Pipeline

numeric_features = [t[0] for t in binary_audio_feature_genre.dtypes if t[1] == 'double']
assembler = VectorAssembler(inputCols=numeric_features, outputCol="VEC-FEATURES")
standard_scaler = StandardScaler(inputCol=assembler.getOutputCol(), outputCol="SCALED_FEATURES")
pca = PCA(k=5, inputCol=standard_scaler.getOutputCol(), outputCol="features")

# use Pipeline to chain multiple Transformers and Estimators together to specify our machine learning workflow

pipeline = Pipeline(stages=[assembler, standard_scaler, pca])
pipelineModel = pipeline.fit(training)
training = pipelineModel.transform(training).select('genre', 'features', 'label')
test = pipelineModel.transform(test).select('genre', 'features', 'label')

# check the training data after transformers

training.show()
test.show()

# Train the Logistic Regression

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression()
lr_model = lr.fit(training)
lr_pred = lr_model.transform(test)
lr_pred.cache()

# Train the Linear Support Vector Machine

from pyspark.ml.classification import LinearSVC

lsvc = LinearSVC(maxIter=10, regParam=0.1)
lsvcModel = lsvc.fit(training)
lsvc_pred = lsvcModel.transform(test)

# Train the Random Forest Classifier

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
rfModel = rf.fit(training)
rf_pred = rfModel.transform(test)

# (e)

# compute the performance matrics for each model

def performance_matrix(df):
    """compute the performance matrics for each model"""
    total = df.count()
    nP = df.filter((F.col('prediction') == 1)).count()
    nN = df.filter((F.col('prediction') == 0)).count()
    TP = df.filter((F.col('prediction') == 1) & (F.col('label') == 1)).count()
    FP = df.filter((F.col('prediction') == 1) & (F.col('label') == 0)).count()
    FN = df.filter((F.col('prediction') == 0) & (F.col('label') == 1)).count()
    TN = df.filter((F.col('prediction') == 0) & (F.col('label') == 0)).count()
    
    print('num positive: {}'.format(nP))
    print('num negative: {}'.format(nN))
    print("True Positives:", TP)
    print("True Negatives:", TN)
    print("False Positives:", FP)
    print("False Negatives:", FN)
    print('accuracy: {}'.format((TP + TN) / total))
        
    if TP == 0:
        print("Precision: 0")
        print("Recall: 0")
    
    else:
        print('recall: {}'.format(TP / (TP + FN)))
        print('precision: {}'.format(TP / (TP + FP)))        
    
# Performance Metrics for LogisticRegression

performance_matrix(lr_pred)

# before subsampling

#num positive: 121
#num negative: 83483
#True Positives: 42
#True Negatives: 75490
#False Positives: 79
#False Negatives: 7993
#accuracy: 0.9034495957131238
#recall: 0.005227131300560049
#precision: 0.34710743801652894

# after subsampling

#num positive: 35484
#num negative: 48120
#True Positives: 5616
#True Negatives: 45701
#False Positives: 29868
#False Negatives: 2419
#accuracy: 0.6138103440026793
#recall: 0.6989421281891723
#precision: 0.15826851538721678

# Performance Metrics for Linear Support Vector Machine

performance_matrix(lsvc_pred)

# before subsampling

#num positive: 1
#num negative: 83603
#True Positives: 0
#True Negatives: 75568
#False Positives: 1
#False Negatives: 8035
#accuracy: 0.9038801971197551
#Precision: 0
#Recall: 0

# after subsampling

#num positive: 50424
#num negative: 33180
#True Positives: 6320
#True Negatives: 31465
#False Positives: 44104
#False Negatives: 1715
#accuracy: 0.4519520597100617
#recall: 0.7865588052271313
#precision: 0.12533714104394733

# Performance Metrics for Random Forest Classifier

performance_matrix(rf_pred)

# before subsampling

#num positive: 0
#num negative: 83604
#True Positives: 0
#True Negatives: 75569
#False Positives: 0
#False Negatives: 8035
#accuracy: 0.9038921582699392
#Precision: 0
#Recall: 0

# after subsampling

#num positive: 29511
#num negative: 54093
#True Positives: 5650
#True Negatives: 51708
#False Positives: 23861
#False Negatives: 2385
#accuracy: 0.6860676522654419
#recall: 0.7031736154324829
#precision: 0.19145403408898376

# (f)

# use cross-validations to tune the hyperparameters for each model

# LogisticRegression
# construct a grid of parameters 

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# define a parameter grid

lr_paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0, 0.01, 0.1]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0, 0.1, 0.5]) # Elastic Net Parameter
             .build())
             
rf_paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [100, 200]) 
             .addGrid(rf.maxDepth, [2, 4]) 
             .addGrid(rf.maxBins, [16, 32])
             .build())
             
lsvc_paramGrid = (ParamGridBuilder()
             .addGrid(lsvc.regParam, [0, 0.01, 0.1]) # regularization parameter
             .build())
             
paramGrids = [lr_paramGrid, rf_paramGrid, lsvc_paramGrid]
             
# create evaluator

binary_evaluator = BinaryClassificationEvaluator()

# apply cross-validations model

estimators = [lr, rf, lsvc]
method = ['Logistic Regression', 'Random Forest', 'Liner Support Vector Machine']

for i in range(len(estimators)):
    cv = CrossValidator(estimator=estimators[i],
                    estimatorParamMaps=paramGrids[i],
                    evaluator=binary_evaluator,
                    numFolds=5)
    cv_model = cv.fit(training)
    cv_prediction = cv_model.transform(test)

    print("Corss-Validation performance of {}:".format(method[i]))
    print("AUC: " , binary_evaluator.evaluate(cv_prediction))
    print("confusion matrix:")
    performance_matrix(cv_prediction)
    
#Corss-Validation performance of Logistic Regression:
#AUC:  0.7062731618127516
#confusion matrix:
#num positive: 37205
#num negative: 46399
#True Positives: 5792
#True Negatives: 44156
#False Positives: 31413
#False Negatives: 2243
#accuracy: 0.5974355294005071
#recall: 0.7208462974486621
#precision: 0.1556780002687811

#Corss-Validation performance of Random Forest:
#AUC:  0.7543045784414106
#confusion matrix:
#num positive: 28572
#num negative: 55032
##True Positives: 5391
#True Negatives: 52388
#False Positives: 23181
#False Negatives: 2644
#accuracy: 0.6911032964929907
#recall: 0.6709396390790292
#precision: 0.1886812263754725

#Corss-Validation performance of Liner Support Vector Machine:
#AUC:  0.678501182767044
#confusion matrix:
#num positive: 50424
#num negative: 33180
#True Positives: 6320
#True Negatives: 31465
#False Positives: 44104
#False Negatives: 1715
#accuracy: 0.4519520597100617
#recall: 0.7865588052271313
#precision: 0.12533714104394733



# Q3

# (a)
# convert genre column into an integer index

audio_genre = audio_feature_genre.drop('track_id')
audio_genre.groupBy('genre').count().orderBy('count', ascending=False).show()
audio_genre.count()

#+--------------+------+
#|         genre| count|
#+--------------+------+
#|      Pop_Rock|237649|
#|    Electronic| 40666|
#|           Rap| 20899|
#|          Jazz| 17775|
#|         Latin| 17504|
#|           RnB| 14314|
#| International| 14194|
#|       Country| 11691|
#|     Religious|  8780|
#|        Reggae|  6931|
#|         Blues|  6801|
#|         Vocal|  6182|
#|          Folk|  5789|
#|       New Age|  4000|
#| Comedy_Spoken|  2067|
#|        Stage |  1613|
#|Easy_Listening|  1535|
#|   Avant_Garde|  1012|
#|     Classical|   555|
#|      Children|   463|
#+--------------+------+


# label index

audio_genre_string = StringIndexer(inputCol = "genre", outputCol = "label")
audio_genre_index = audio_genre_string.fit(audio_genre).transform(audio_genre)
audio_genre_index.show(5)

#+------+-----+-----+-------+---------+------+-----+------+--------+---------+----------+-----+
#|   _c0|  _c1|  _c2|    _c3|      _c4|   _c5|  _c6|   _c7|     _c8|      _c9|     genre|label|
#+------+-----+-----+-------+---------+------+-----+------+--------+---------+----------+-----+
#|0.1308|9.587|459.9|27280.0|4303000.0|0.2474|26.02|1067.0| 67790.0|8281000.0|  Pop_Rock|  0.0|
#|0.1208|6.738|215.1|11890.0|2278000.0|0.4882|41.76|2164.0|220400.0|   3.79E7|  Pop_Rock|  0.0|
#|0.2838|8.995|429.5|31990.0|5272000.0|0.5388|28.29|1656.0|185100.0|  3.164E7|  Pop_Rock|  0.0|
#|0.1346|7.321|499.6|38460.0|5877000.0|0.2839|15.75| 929.6|116500.0|  2.058E7|      Jazz|  3.0|
#|0.1563|9.959|502.8|26190.0|3660000.0|0.3835|28.24|1864.0|180800.0|  2.892E7|Electronic|  1.0|
#+------+-----+-----+-------+---------+------+-----+------+--------+---------+----------+-----+
#only showing top 5 rows


# (c)
# random split data to training data and test data

training, test =  audio_genre_index.randomSplit([0.8, 0.2], seed=1000)
training.groupBy('label', 'genre').count().orderBy('count', ascending=False).show()

#+-----+--------------+------+
#|label|         genre| count|
#+-----+--------------+------+
#|  0.0|      Pop_Rock|190488|
#|  1.0|    Electronic| 32631|
#|  2.0|           Rap| 16659|
#|  3.0|          Jazz| 14276|
#|  4.0|         Latin| 14040|
#|  5.0|           RnB| 11478|
#|  6.0| International| 11351|
#|  7.0|       Country|  9325|
#|  8.0|     Religious|  7044|
#|  9.0|        Reggae|  5557|
#| 10.0|         Blues|  5448|
#| 11.0|         Vocal|  4953|
#| 12.0|          Folk|  4624|
#| 13.0|       New Age|  3213|
#| 14.0| Comedy_Spoken|  1657|
#| 15.0|        Stage |  1266|
#| 16.0|Easy_Listening|  1210|
#| 17.0|   Avant_Garde|   814|
#| 18.0|     Classical|   441|
#| 19.0|      Children|   381|
#+-----+--------------+------+
#only showing top 20 rows


test.groupBy('label', 'genre').count().orderBy('count', ascending=False).show()

#+-----+--------------+-----+
#|label|         genre|count|
#+-----+--------------+-----+
#|  0.0|      Pop_Rock|47161|
#|  1.0|    Electronic| 8035|
#|  2.0|           Rap| 4240|
#|  3.0|          Jazz| 3499|
#|  4.0|         Latin| 3464|
#|  6.0| International| 2843|
#|  5.0|           RnB| 2836|
#|  7.0|       Country| 2366|
#|  8.0|     Religious| 1736|
#|  9.0|        Reggae| 1374|
#| 10.0|         Blues| 1353|
#| 11.0|         Vocal| 1229|
#| 12.0|          Folk| 1165|
#| 13.0|       New Age|  787|
#| 14.0| Comedy_Spoken|  410|
#| 15.0|        Stage |  347|
#| 16.0|Easy_Listening|  325|
#| 17.0|   Avant_Garde|  198|
#| 18.0|     Classical|  114|
#| 19.0|      Children|   82|
#+-----+--------------+-----+
#only showing top 20 rows

# Preparing Data for Machine Learning

numeric_features = [t[0] for t in audio_genre.dtypes if t[1] == 'double']
assembler = VectorAssembler(inputCols=numeric_features, outputCol="VEC-FEATURES")
standard_scaler = StandardScaler(inputCol=assembler.getOutputCol(), outputCol="SCALED_FEATURES")
pca = PCA(k=5, inputCol=standard_scaler.getOutputCol(), outputCol="features")

# use Pipeline to chain multiple Transformers and Estimators together to specify our machine learning workflow

pipeline = Pipeline(stages=[assembler, standard_scaler, pca])
pipelineModel = pipeline.fit(training)
training = pipelineModel.transform(training).select('genre', 'features', 'label')
test = pipelineModel.transform(test).select('genre', 'features', 'label')

# build logisiticregreesion model and make prediciton

lr = LogisticRegression()
lr_model = lr.fit(training)
lr_pred = lr_model.transform(test)

# create MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator()

# cross validaition for logistic regession

LR_paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0, 0.01, 0.1]) 
             .addGrid(lr.elasticNetParam, [0, 0.1, 0.2]) 
             .build())

cross_validator = CrossValidator(estimator=lr,
                    estimatorParamMaps=LR_paramGrid,
                    evaluator=evaluator,
                    numFolds=5)

cv_model = cross_validator.fit(training)
cv_pred = cv_model.transform(test)

# evaluate the accuracy

evaluator.evaluate(cv_pred) 

#accuracy 
#0.5691713315152385



