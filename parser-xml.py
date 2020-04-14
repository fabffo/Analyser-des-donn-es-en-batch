# coding: utf-8

from lxml import etree

tree = etree.parse("/Users/fabricefougery/Formation_dataArchitecte/4_Donnees_en_batch/datawiki/extraitXmlHistory.txt")
for title in tree.xpath("/pages/page/title"):
    print(title.text)