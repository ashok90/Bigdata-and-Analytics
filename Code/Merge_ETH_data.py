from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("RandomForest").setMaster("local[4]")
sc = SparkContext(conf=conf)

from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

import pyspark.mllib
import pyspark.mllib.regression
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import *
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


data1=sc.textFile('/Users/ashokkuppuraj/Documents/Indiana Univ/fall 2017/BIG DATA/Project/scripts/data/export-ETHTx.csv')
data2=sc.textFile('/Users/ashokkuppuraj/Documents/Indiana Univ/fall 2017/BIG DATA/Project/scripts/data/export-EtherPrice.csv')
rdd1 = data1.map(lambda line: line.split(","))
rdd2 = data2.map(lambda line: line.split(","))
header1 = rdd1.first()
header2 = rdd2.first()
rdd1 = rdd1.filter(lambda line:line != header1)
rdd2 = rdd2.filter(lambda line:line != header2)
rdd1.take(10)

ETH_TXN_df = rdd1.map(lambda line: Row(dt=line[0],utime=line[1],txns=line[2])).toDF()
ETH_PRICE_df = rdd2.map(lambda line: Row(dt=line[0],utime=line[1],price=line[2])).toDF()

ETH_df=ETH_TXN_df.join(ETH_PRICE_df,ETH_TXN_df.dt==ETH_PRICE_df.dt)