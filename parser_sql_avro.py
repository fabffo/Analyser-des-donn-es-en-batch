# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import re, ast, sys
import fastavro
import hdfs

def main(input, output):
        rdd = sc.textFile(input)
        brut = rdd.filter(lambda ligne: len(ligne) > 0 and ligne[0] in 'I')
        rdde = brut.map(lambda line: re.compile("INSERT INTO `pagelinks` VALUES ").sub("", line))
        rddee = rdde.map(lambda line: re.compile("[;\s]").sub("", line))

        a1 = rddee.map(lambda x: x.split("),("))
        a2 = a1.flatMap(lambda x: (x[v] for v in range(len(x))))
        a3 = a2.map(lambda x: (re.compile('\(').sub('', str(x))))
        a4 = a3.map(lambda x: (re.compile('\)').sub('', str(x))))
        a5 = a4.map(lambda x: (ast.literal_eval(x)))
        a6 = a5.map(lambda x: (int(x[0]), x[2]))


        # Convert to a Spark dataframe
        df = a6.toDF(['page_id', 'page_title'])

        # Cache data to avoid re-computing everything
        df.persist()

        # Writing
        parsed_schema = df.schema
        print(parsed_schema)

        df.write.format("avro").save("hdfs://localhost:9000/data-wiki/raw/frwiki-20191220-pagesql.avro")


if __name__ == "__main__":
        sc = SparkContext()
        spark = SparkSession.builder.getOrCreate()

        # récupération des paramétres
        input = sys.argv[1]
        output = sys.argv[2]

        # on lance le main
        main(input, output)
