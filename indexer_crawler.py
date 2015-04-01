import os
from os.path import join
import re 
import elasticsearch
from elasticsearch import client
from elasticsearch.client.cat import CatClient

es = elasticsearch.Elasticsearch("localhost:9200", timeout=600, maxRetry=2, revival_delay=0)
index = elasticsearch.client.IndicesClient(es)
catClient = elasticsearch.client.CatClient(es)

def deleteIndex ():
    index.delete('*')

def createIndex():
        index.create(index='ap_dataset',
                 body={
                          "settings": {
                            "index": {
                              "store": {
                                "type": "default"
                              },
                              "number_of_shards": 1,
                              "number_of_replicas": 1
                            },
                            "analysis": {
                              "analyzer": {
                                "my_english": { 
                                  "type": "english",
                                  "stopwords_path": "stoplist.txt" 
                                }
                              }
                            }
                          }
                        })
        
        index.put_mapping(index='ap_dataset', doc_type = 'document', body={
                                                      "document": {
                                                        "properties": {   
                                                        "urlId": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                          },                                                                                                                       
                   
                                                        "text": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "analyzed",
                                                            "term_vector": "with_positions_offsets_payloads",
                                                            "analyzer": "my_english"
                                                         },
                                                                       
                                                        "inlinks": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "no"
                                                          },
                                                                       
                                                        "outlinks": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "no"
                                                          }             
                                                    
                                                        }
                                                      }
                                                    })
    
def readDocumentList():
    
    path = "crawler/newCrawler"
    listOfFiles = os.listdir(path);
    documentIds = []
    j = 0
    for file in listOfFiles:
        i = 0
        f = open(join(path, file), "r").read()
        documentNumbers = getURLId(f)
        corpusTextContent = getTextInfo(f)
        inlinks = getInlinksInfo(f)
        outlinks = getOutLinksInfo(f)
        addDocumentToIndex(documentNumbers, corpusTextContent, inlinks, outlinks)


def getInlinksInfo(f):
    inlinksContent = ""
    inlinks = re.findall('<INLINKS>.*?</INLINKS>', f, re.DOTALL)
    for inlink in inlinks:
        removedTagString = re.sub('<.*?>', '', inlink)
        inlinksContent = inlinksContent + removedTagString
    return inlinksContent

def getOutLinksInfo(f):
    outlinksContent = ""
    outlinks = re.findall('<OUTLINKS>.*?</OUTLINKS>', f, re.DOTALL)
    for outlink in outlinks:
        removedTagString = re.sub('<.*?>', '', outlink)
        outlinksContent = outlinksContent + removedTagString
    return outlinksContent
            
def addDocumentToIndex(docId, corpusContent, inlinks, outlinks):
    listOfInlinks = inlinks.split('\n')
    setOfInlinks = set(listOfInlinks)
    
    try:
        getInlinksFromElasticSearch = es.get(index = 'ap_dataset',doc_type = 'document', id = docId)
        setOfInlinks = setOfInlinks.union(set(getInlinksFromElasticSearch['_source']['inlinks']).split('\n')) 
    except:
        pass
    
    finally:
        es.index(
                index = 'ap_dataset', 
                doc_type = 'document', 
                id = docId, 
                body = {
                'docno': docId,
                'text': corpusContent,
                'inlinks': '\n'.join(setOfInlinks),
                'outlinks': outlinks
                })

    
def getTextInfo(d):
    textContent = ""
    text = re.findall('<TEXT>.*?</TEXT>', d, re.DOTALL)
    for elem in text:
        removedTagString = re.sub('<.*?>', '', elem)
        textContent = textContent + removedTagString
    return textContent

def getURLId(f):
    url = ""
    docNo = re.findall('<URL>.*</URL>', f,re.DOTALL)
    for d in docNo:
        removedTagString = re.sub('<.*?>', '', d)
        removedTagString = removedTagString.strip()
        url = url + removedTagString
    return url


def main():
    deleteIndex()
    createIndex()
    readDocumentList()
    
main()