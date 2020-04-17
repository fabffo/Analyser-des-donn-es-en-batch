# coding: utf-8

# from lxml import etree
#
# tree = etree.parse("/Users/fabricefougery/Formation_dataArchitecte/4_Donnees_en_batch/datawiki/extraitXmlHistory.txt")
#
# for title in tree.xpath("/pages/page/title"):
#     print(title.text)

import xml.etree.ElementTree as ET
import json

#initialisation des listes
list_revision = []
list_page_title = []
list_page_revision = []
list_pages = []
dico_page = []

#initialisation des indices tableaux
d=0
c=0
cs=0
ds=0
n=0
ns=0

#initialisation des tableaux
dico_title= {}
dico_id= {}
tab_contributor_id = {}
tab_contributor_name = {}
tab_indice_contributor= {}

#récupération de la racine du fichier xml
tree = ET.parse("/Users/fabricefougery/Formation_dataArchitecte/4_Donnees_en_batch/datawiki/extrait.txt")
root = tree.getroot()

#Boucle sur les noeuds
for child in root:
    list_revision = []
    dico = {}
    for page in child:
        if page.tag=='revision':
            for revision in page:
                if revision.tag == "contributor":
                    tab_indice_contributor[c] = 0
                    for contributor in revision:
                        if contributor.tag == "id":
                            tab_contributor_id[c] = ("id_contributor", contributor.text)
                            list_revision.append(tab_contributor_id[c])
                        if contributor.tag == "username":
                            n = n+1
                            tab_indice_contributor[c] = n
                            tab_contributor_name[n] = ("username_contributor", contributor.text)
                            list_revision.append(tab_contributor_name[n])
                    c = c + 1
        else:
            if (page.tag=="title" ):
                d = d + 1
                dico_title[d] = ("tilte", page.text)
            if (page.tag=="id" ):
                dico_id[d] = ("id_title", page.text)


    while ds < d:
        ds=ds+1
        list_page_title.append(dico_title[ds])
        list_page_title.append(dico_id[ds])
    ds=d
    #print (ds)

    while cs < c:
         if tab_indice_contributor[cs]>0:
             list_page_revision.append(tab_contributor_name[tab_indice_contributor[cs]])
         list_page_revision.append(tab_contributor_id[cs])
         cs = cs + 1

    #print(list_page)

    list_pages.append(list_page_title)
    list_pages.append(list_page_revision)

    data = json.dumps(list_pages)
    print(data)
    with open("history.json", "a") as f:
        f.write(data)
    list_page_title[:] = []
    list_page_revision[:] = []
    list_pages[:] = []