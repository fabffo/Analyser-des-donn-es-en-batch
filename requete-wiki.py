# 3 paramètres le fichier historique, le fichier sql, le sujet
#spark-submit --master local[4] --driver-memory=6g --packages org.apache.spark:spark-avro_2.11:2.4.4 requete-wiki.py hdfs://localhost:9000/data-wiki/work/historique.avro hdfs://localhost:9000/data-wiki/work/pagesql.avro 'Cinéma surréaliste'
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.conf import SparkConf
import fastavro
from io import BytesIO
import sys, collections
from pyspark.sql.functions import explode
import re


def main(sujet):
    #conf = SparkConf()
    #conf.set("spark.executor.memory", "4g")
    #sc = SparkContext(conf=conf)

    sc = SparkContext()
    spark = SparkSession.builder.getOrCreate()

    # Load files
    #rdd = sc.binaryFiles('hdfs://localhost:9000/data-wiki/work/historique.avro')
    rdd = sc.binaryFiles(parm_histo)

    # Parse avro files
    nodes = rdd.flatMap(lambda args: fastavro.reader(BytesIO(args[1])))

    # Convert to a resilient distributed dataset (RDD) of rows
    rows = nodes.map(lambda node: Row(**node))

    # Convert to a Spark dataframe
    df = spark.createDataFrame(rows, samplingRatio=1)

    # Cache data to avoid re-computing everything
    df.persist()

    historique = df
    #liensql =  spark.read.format("avro").load("hdfs://localhost:9000/data-wiki/work/pagesql.avro")
    liensql = spark.read.format("avro").load(parm_sql)

    # Récupération des contributeurs du sujet
    sel_historique = historique.filter(historique.title == sujet)
    title_historique = (sel_historique.first().title)
    id_historique = (sel_historique.first().id)
    dt = sel_historique.select(explode(sel_historique.contributors)).groupBy("col").count()

    # liens historiques précédents
    liensql_from = liensql.filter(liensql.page_title == sujet).join(historique, (historique.id == liensql.page_id))
    df = liensql_from.select(explode(liensql_from.contributors)).groupBy("col").count()
    dtf = df.unionAll(dt).orderBy('count', ascending=False)

    # liens historiques suivants
    liensql_to = liensql.filter(liensql.page_id == id_historique).join(historique, (historique.title == liensql.page_title))
    dt = liensql_to.select(explode(liensql_to.contributors)).groupBy("col").count()
    dc = dtf.unionAll(dt).orderBy('count', ascending=False)
 
    dc.createTempView("datasql")
    spark.sql("SELECT col as contributeur, count as score FROM datasql limit 3").show()
    print("Les meilleurs contributeurs pour le sujet wikipédia : " + sys.argv[3])


if __name__ == "__main__":
    # récupération des paramétres
    parm_histo = sys.argv[1]
    parm_sql = sys.argv[2]
    sujet = sys.argv[3]
    sujet = re.compile('\s').sub("", str(sujet))


    #on lance le main
    main(sujet)