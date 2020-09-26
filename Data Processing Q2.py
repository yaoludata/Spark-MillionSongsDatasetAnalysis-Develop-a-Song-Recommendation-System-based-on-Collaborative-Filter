# DATA420-19S2 Assignment 2

# Data Processing
# Q2

# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()


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

#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
#|           song_id|        song_artist|                              song_title|          track_id|  track_artist|                             track_title|
#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
#|SOUMNSI12AB0182807|Digital Underground|                        The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|           Whats up with the Underground|
#|SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shining (Digitally Remaste...|TRMMREB12903CEB1B1|    Slim Harpo|               I Got Love If You Want It|
#|SOLPHZY12AC468ABA8|      Africa HiTech|                                Footstep|TRMMBOC12903CEB46E|Marcus Worgull|                 Drumstern (BONUS TRACK)|
#|SONGHTM12A8C1374EF|     Death in Vegas|                            Anita Berber|TRMMITP128F425D8D0|     Valen Hsu|                                  Shi Yi|
#|SONGXCA12A8C13E82E| Grupo Exterminador|                           El Triunfador|TRMMAYZ128F429ECE6|     I Ribelli|                               Lei M'Ama|
#|SOMBCRC12A67ADA435|      Fading Friend|                             Get us out!|TRMMNVU128EF343EED|     Masterboy|                      Feel The Heat 2000|
#|SOTDWDK12A8C13617B|       Daevid Allen|                              Past Lives|TRMMNCZ128F426FF0E| Bhimsen Joshi|            Raga - Shuddha Sarang_ Aalap|
#|SOEBURP12AB018C2FB|  Cristian Paduraru|                              Born Again|TRMMPBS12903CE90E1|     Yespiring|                          Journey Stages|
#|SOSRJHS12A6D4FDAA3|         Jeff Mills|                      Basic Human Design|TRMWMEL128F421DA68|           M&T|                           Drumsettester|
#|SOIYAAQ12A6D4F954A|           Excepter|                                      OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (Natchs Scheint Die So...|
#+------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
#only showing top 10 rows

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

#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+
#|           song_id|      song_artist|                              song_title|          track_id|                            track_artist|                             track_title|
#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+
#|SOFQHZM12A8C142342|     Josipa Lisac|                                 razloga|TRMWMFG128F92FFEF2|                            Lisac Josipa|                            1000 razloga|
#|SODXUTF12AB018A3DA|       Lutan Fyah|     Nuh Matter the Crisis Feat. Midnite|TRMWPCD12903CCE5ED|                                 Midnite|                   Nah Matter the Crisis|
#|SOASCRF12A8C1372E6|Gaetano Donizetti|L'Elisir d'Amore: Act Two: Come sen v...|TRMHIPJ128F426A2E2|Gianandrea Gavazzeni_ Orchestra E Cor...|L'Elisir D'Amore_ Act 2: Come Sen Va ...|
#|SOITDUN12A58A7AACA|     C.J. Chenier|                               Ay, Ai Ai|TRMHXGK128F42446AB|                         Clifton Chenier|                               Ay_ Ai Ai|
#|SOLZXUM12AB018BE39|           許志安|                                男人最痛|TRMRSOF12903CCF516|                                Andy Hui|                        Nan Ren Zui Tong|
#|SOTJTDT12A8C13A8A6|                S|                                       h|TRMNKQE128F427C4D8|                             Sammy Hagar|                 20th Century Man (Live)|
#|SOGCVWB12AB0184CE2|                H|                                       Y|TRMUNCZ128F932A95D|                                Hawkwind|                25 Years (Alternate Mix)|
#|SOKDKGD12AB0185E9C|     影山ヒロノブ|Cha-La Head-Cha-La (2005 ver./DRAGON ...|TRMOOAH12903CB4B29|                        Takahashi Hiroki|Maka fushigi adventure! (2005 Version...|
#|SOPPBXP12A8C141194|    Αντώνης Ρέμος|                        O Trellos - Live|TRMXJDS128F42AE7CF|                           Antonis Remos|                               O Trellos|
#|SODQSLR12A8C133A01|    John Williams|Concerto No. 1 for Guitar and String ...|TRWHMXN128F426E03C|               English Chamber Orchestra|II. Andantino siciliano from Concerto...|
#+------------------+-----------------+----------------------------------------+------------------+----------------------------------------+----------------------------------------+
#only showing top 10 rows

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

#+----------------------------------------+------------------+-----+
#|                                 user_id|           song_id|plays|
#+----------------------------------------+------------------+-----+
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQEFDN12AB017C52B|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOIUJ12A6701DAA7|    2|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOKKD12A6701F92E|    4|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSDVHO12AB01882C7|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSKICX12A6701F932|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSNUPV12A8C13939B|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSVMII12A6701F92D|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTUNHI12B0B80AFE2|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTXLTZ12AB017C535|    1|
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTZDDX12A6701F935|    1|
#+----------------------------------------+------------------+-----+
#only showing top 10 rows

# remove the mismatches data which is in manually accepted data

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti") 

# remove the mismatches song from Taste Profile dataset

triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

print(mismatches_not_accepted.count())  # 19093
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111

# (b)

# Processing Q2 (b)

# hdfs dfs -cat "/data/msd/audio/attributes/*" | awk -F',' '{print $2}' | sort | uniq

# NUMERIC
# real
# real
# string
# string
# STRING

audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

audio_dataset_names = [
  "msd-jmir-area-of-moments-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-moments-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

audio_datasets = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2019/DATA420-19S2/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

  audio_dataset_schema = StructType([
    StructField(
      row[0],
      audio_attribute_type_mapping[row[1]],
      True
    ) for row in rows
  ])

  audio_datasets[audio_dataset_name] = (
    spark.read.format("csv")
    .option("header", "false")
    .option("codec", "gzip")
    .schema(audio_dataset_schema)
    .load(f"/data/msd/audio/features/{audio_dataset_name}.csv")
  )

# Example

features = audio_datasets["msd-jmir-area-of-moments-all-v1.0"]

features.printSchema()
print(features.count())  # 994623

# root
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_1: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_2: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_3: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_4: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_5: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_6: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_7: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_8: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_9: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Standard_Deviation_10: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_1: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_2: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_3: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_4: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_5: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_6: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_7: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_8: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_9: double (nullable = true)
#  |-- Area_Method_of_Moments_Overall_Average_10: double (nullable = true)
#  |-- MSD_TRACKID: string (nullable = true


