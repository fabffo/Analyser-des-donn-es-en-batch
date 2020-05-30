# coding: utf-8
import hdfs
import fastavro
import re
import sys

def main(input, output):
    client = hdfs.InsecureClient("http://0.0.0.0:50070")
    hdfs_client = hdfs.InsecureClient("http://0.0.0:50070")
    # Lecture d'un fichier complet
    with client.read(input, encoding="utf-8") as reader:
        tab_sujet = {}
        list_page = []
        list_contributor = []
        debut = True
        # Lecture d'un fichier complet
        # with client.read("/data-wiki/raw/frwiki-20191220-stub-meta-history.xml") as reader:
        for line in reader:
            if re.compile('\s<page>').search(str(line)):
                bool_page = True
                if debut:
                    debut = False
                else:
                    tab_sujet["contributors"] = list_contributor
                    list_page.append(tab_sujet)
                    tab_sujet = {}
                    list_contributor = []

            if re.compile('\s<title>').search(str(line)) and bool_page:
                bool_tilte = True
                bool_page = False
                line = re.compile('<title>').sub("", str(line))
                line = re.compile('<\/title>').sub("", str(line))
                line = re.compile("\\\\n\'+").sub('', str(line))
                line = re.compile("").sub('', str(line))
                line = re.compile('\s').sub("", str(line))
                tab_sujet["title"] = line

            if (re.compile('\s<id>').search(str(line))) and bool_tilte:
                line = re.compile('<id>').sub("", str(line))
                line = re.compile('<\/id>').sub("", str(line))
                line = re.compile("\\\\n\'+").sub('', str(line))
                line = re.compile("").sub('', str(line))
                line = re.compile('\s').sub("", str(line))
                tab_sujet["id"] = line
                bool_tilte = False

            if re.compile('\s<revision>').search(str(line)):
                bool_revision = True

            if re.compile('\s<contributor>').search(str(line)) and bool_revision:
                bool_contributor = True

            if (re.compile('\s<username>').search(str(line))) and bool_revision and bool_contributor:
                line = re.compile('<username>').sub("", str(line))
                # line = re.compile('^b\'').sub("", str(line))
                line = re.compile('<\/username>').sub("", str(line))
                line = re.compile("\\\\n\'+").sub('', str(line))
                line = re.compile("").sub('', str(line))
                line = re.compile('\s').sub("", str(line))
                list_contributor.append(line)
                bool_revision = False
                bool_contributor = False

    schema = {
        "namespace": "ffo.historiqueWiki",
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "id", "type": "string"},
            {"name": "contributors", "type": {"type": "array", "items": "string"}, "default": {}},
        ]
    }
    with hdfs_client.write(output, overwrite=True) as avro_file:
        fastavro.writer(avro_file, schema, list_page)
        print("ok")

if __name__ == "__main__":
    # récupération des paramétres
    input = sys.argv[1]
    output = sys.argv[2]

    #on lance le main
    main(input, output)
