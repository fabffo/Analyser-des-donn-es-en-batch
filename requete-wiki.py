#spark-submit --master local[4] --driver-memory=6g
# analyse-historique-wiki.py PolitiqueenArgentine# 'PolitiqueenArgentine'
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.conf import SparkConf
import fastavro
from io import BytesIO
import sys, collections
from pyspark.sql.functions import explode


def main(input):
    #conf = SparkConf()
    #conf.set("spark.executor.memory", "4g")
    #sc = SparkContext(conf=conf)

    sc = SparkContext()
    spark = SparkSession.builder.getOrCreate()

    # Load files
    rdd = sc.binaryFiles('hdfs://localhost:9000/data-wiki/work/historique.avro')  # (filename, content)
    # If it takes too long to process all files, you may want to reduce the number
    # of processed files. E.g:
    # rdd = sc.binaryFiles('hdfs://localhost:9000/data/paris/master/full/2.250182*.avro') # (filename, content)

    # Parse avro files
    nodes = rdd.flatMap(lambda args: fastavro.reader(BytesIO(args[1])))

    # Convert to a resilient distributed dataset (RDD) of rows
    rows = nodes.map(lambda node: Row(**node))

    # Convert to a Spark dataframe
    df = spark.createDataFrame(rows, samplingRatio=1)

    # Cache data to avoid re-computing everything
    df.persist()

    historique = df
    liensql =  spark.read.format("avro").load("hdfs://localhost:9000/data-wiki/work/pagesql.avro")

    print("=========== Résultat ============")
    print("le paramètre: ", input)
    # Récupération des contributeurs du sujet
    sel_historique = historique.filter(historique.title == input)
    title_historique = (sel_historique.first().title)
    id_historique = (sel_historique.first().id)
    dt = sel_historique.select(explode(sel_historique.contributors)).groupBy("col").count()

    # liens historiques précédents
    liensql_from = liensql.filter(liensql.page_title == input).join(historique, (historique.id == liensql.page_id))
    df = liensql_from.select(explode(liensql_from.contributors)).groupBy("col").count()
    dtf = df.unionAll(dt).orderBy('count', ascending=False)

    # liens historiques suivants
    liensql_to = liensql.filter(liensql.page_id == id_historique).join(historique, (historique.title == liensql.page_title))
    dt = liensql_to.select(explode(liensql_to.contributors)).groupBy("col").count()
    dc = dtf.unionAll(dt).orderBy('count', ascending=False)

    resultat = dc.take(3)
    print("=========================== Résultat =========================== ")
    print("le paramètre: ", input)
    print("Vous avez demandé les meilleurs contributeurs pour le sujet wikipédia : " + title_historique)
    print(resultat)
    print("=========================== Résultat =========================== ")



    #     # Récupération des contributeurs du sujet
    # #input = 'PolitiqueenArgentine'
    # #textcote = '"'+ input + '"'
    # print('==============================================================')
    # sel_historique = historique.filter(historique.title == input )
    # title_historique = (sel_historique.first().title)
    # id_historique = (sel_historique.first().id)
    # print("le titre historique: ", title_historique, "avec son id:", id_historique)
    # historique_contributor = collections.Counter(sel_historique.first().contributors)
    # print("les contributeurs: ", historique_contributor)
    # print('==============================================================')

    # # liens historiques précédents , truncate=False
    # liensql = liens.withColumn("page_idTmp", liens.page_id.cast("string")).drop("page_id").withColumnRenamed("page_idTmp","page_id")
    # print("==== les liens ============")
    # liensql_from = liensql.filter(liensql.page_title == input).join(historique, (historique.id == liensql.page_id))
    # contributor_from = (liensql_from.select(liensql_from.contributors).take(liensql_from.count()))
    # i = 0
    # coll_from = collections.Counter()
    # while i < len(contributor_from):
    #     coll_from = coll_from + collections.Counter(contributor_from[i][0])
    #     i = i + 1
    # print(coll_from)
    #
    # # liens historiques suivants
    # #liensql = liens.withColumn("page_idTmp", liens.page_id.cast("string")).drop("page_id").withColumnRenamed("page_idTmp",                                                                                                   "page_id")
    # print("==== les liens to ============")
    # liensql_to = liensql.filter(liensql.page_id == id_historique).join(historique, (historique.title == liensql.page_title))
    # contributor_to = (liensql_to.select(liensql_to.contributors).take(liensql_to.count()))
    # i = 0
    # coll_to = collections.Counter()
    # while i < len(contributor_to):
    #     coll_to = coll_to + collections.Counter(contributor_to[i][0])
    #     i = i + 1
    # print(coll_to)
    # contributor_total = historique_contributor + coll_from + coll_to
    #
    # print("=========== Résultat ============")
    # print("Vous avez demandé les meilleurs contributeurs pour le sujet wikipédia : " + title_historique)
    # print(contributor_total.most_common(10))
    # print("=========== Résultat ============")

if __name__ == "__main__":
    # récupération des paramétres
    input = sys.argv[1]


    #on lance le main
    main(input)