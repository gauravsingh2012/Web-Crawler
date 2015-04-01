from urlparse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
from collections import deque
from sets import Set
import sys
import time
import robotparser

reload(sys)  
sys.setdefaultencoding('UTF8')
visitedUrls = Set([])

class node():
    
    def __init__(self, url):
        self.url = url
        self.inLinks = []
        self.outLinks = []
        self.fileNumber = -1
        
    def setInLink(self, inLink):
        self.inLinks.append(inLink)
        
    def setOutLink(self, outLink):
        self.outLinks.append(outLink)
    
    def setFileNumber(self, fileNumber):
        self.fileNumber = fileNumber
    
    def getInLinks(self):
        return self.inLinks
    
    def getOutLinks(self):
        return self.outLinks
        
    def getURL(self):
        return self.url

    def getFileNumber(self):
        return self.fileNumber


def addSeedUrlsToQueue():
    
    seedURL1 = node('http://www.nhc.noaa.gov/outreach/history/') 
    seedURL2 = node('http://en.wikipedia.org/wiki/Hurricane_Ike')
    seedURL3 = node('http://www.srh.noaa.gov/hgx/?n=projects_ike08')    
    #queue = deque([seedURL2,seedURL3,seedURL1])
    queue = deque([seedURL3,seedURL1])
    
    return queue

def getURLContents(urlNode,queue,fileNumber,urlDict):
    content = ""
    title = ""
    inLinkTupleList = []
    try:
        page_content = requests.get(urlNode.getURL())
        
        if page_content.headers['content-type'] == 'text/html' or page_content.headers['content-type'] == 'text/html; charset=UTF-8':
            if page_content.headers['content-type'] == 'text/html':
                soup = BeautifulSoup(page_content.text.encode('utf-8'))
            else:
                soup = BeautifulSoup(page_content.text)
            
            visitedUrls.add(urlNode.getURL())
            
            for link in soup.findAll('a',href=True):
                canonicalizedURL = url_canonicalization(urlNode.getURL(), link['href'].encode('utf-8')) 
                outLinkNode = node(canonicalizedURL)
                if (canonicalizedURL != urlNode.getURL()):
                    updateInlink(urlNode, outLinkNode, inLinkTupleList ,urlDict)
                    urlNode.setOutLink(outLinkNode.getURL())
                
            urlDict[urlNode.getURL()] = urlNode
            for domTitle in soup.find_all('title'):
                title = title +  domTitle.get_text().encode('utf-8')
            for text in soup.find_all('p'):
                content = content +  text.get_text().encode('utf-8')
            
            addToQueue(inLinkTupleList, queue)
            writeToFile(urlNode,title,content,page_content,fileNumber)
            
    except Exception as e:
        pass
    except requests.exceptions.ConnectionError,e:
        pass


def addToQueue(inLinkTupleList, queue):
    for tup in sorted(inLinkTupleList, reverse=True):
        queue.append(tup[1])

def updateInlink(urlNode, outLinkNode , inLinkTupleList, urlDict):
    if urlDict.has_key(outLinkNode.getURL()):
        urlObject = urlDict.get(outLinkNode.getURL())
        inLinks = urlObject.getInLinks()
        if urlNode.getURL() not in inLinks:
            urlObject.setInLink(urlNode.getURL())
        urlDict[outLinkNode.getURL()] = urlObject
    else:
        urlObject = outLinkNode
        outLinkNode.setInLink(urlObject.getURL())
        urlDict[outLinkNode.getURL()] = urlObject
        
    tup = (len(urlObject.getInLinks()),outLinkNode)
    inLinkTupleList.append(tup)

def writeToFile(urlNode,title,content,page_content,fileNumber):
    filePath = "crawler/crawledDocument" + str(fileNumber)
    file = open(filePath, "w")
    file.write("<URL>" + urlNode.getURL() + "</URL>" + "\n" + "<TITLE>" + title + "</TITLE>" + "\n" + "<TEXT>" + content + "</TEXT> \n <RAWHTML>" +  page_content.text + "</RAWHTML> \n")

def crawl():
    urlDict = {}
    fileNumber = 0
    queue = addSeedUrlsToQueue()
    for node in queue:
        urlDict[node.getURL()] = node
        
    while True and len(visitedUrls) < 5000:
        
        urlNode = queue.popleft()
        
        if urlNode.getURL() not in visitedUrls:
            print str(fileNumber) , urlNode.getURL() 
            getURLContents(urlNode,queue,fileNumber,urlDict)
            urlNode.setFileNumber(fileNumber)
            fileNumber = fileNumber + 1

        time.sleep(1)
        
    writeLinksToFile(urlDict)
    
def writeLinksToFile(urlDict):
        
    for url in visitedUrls:
        urlObject = urlDict[url]
        file = open('crawler/crawledDocument' + str(urlObject.getFileNumber()), "a")
        
        file.write("<INLINKS>")
        for inLink in urlObject.getInLinks():
            file.write(inLink + "\n")
        file.write("</INLINKS>\n")
        
        file.write("<OUTLINKS>")
        for outLink in urlObject.getOutLinks():
            file.write(outLink + "\n")
        file.write("</OUTLINKS>\n")
        
        
def url_canonicalization(url, outlink_url):
    
    url_attr = urlparse(url)
    
    outlink_url_attr = urlparse(outlink_url)
    
    if not outlink_url_attr.scheme:
        scheme = url_attr.scheme.lower()
    else:
        scheme = url_attr.scheme.lower()
        
    if not outlink_url_attr.netloc:
        netloc = url_attr.netloc.lower()
    else:
        netloc = url_attr.netloc.lower()
        
    canonicalized_url = scheme + '://' + netloc + outlink_url_attr.path
    
    outlink_url_attr = urlparse(canonicalized_url)            
    
    if (outlink_url_attr.port):
        scheme_port_number = outlink_url_attr.netloc.split(':')
        outlink_url_attr = urlparse(outlink_url_attr.scheme.lower() + '://' + scheme_port_number[0].lower() + outlink_url_attr.path)
    
    if '#' in outlink_url:
        return url
    
    if '..' in outlink_url:
        canonicalized_url = urljoin(url,outlink_url)
    
    return canonicalized_url

#url_canonicalization('http://www.example.com:80/b.html', '/c.html')
def main():
    crawl()
    #getURLContents(node('http://en.wikipedia.org/wiki/Hurricane_Ike'), [], 0)
    
main()