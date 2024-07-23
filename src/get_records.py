"""
Utility functions that search for, and download Common Crawl records.

CC records are stored on Amazon servers in WARC format, which contains gzip compressed html files along with some useful metadata about the size of the entry and the date on which it was scraped.

To retrieve Common Crawl data corresponding to a particular date, one must specifiy the 'index', which has the format of '2018-13', '2018-09' and thelike. The full list of indexes can be found here: http://index.commoncrawl.org/. A default index list is hardcoded into the 'get_records_gen' function.
"""
#%%
from typing import List,Dict
from urllib.parse import quote
import requests
import json
from io import BytesIO
import gzip
import re

# make domain useable as filepath
def strip_domain(domain:str) -> str:
	x = domain.replace('*.','')
	x = x.replace('.','_')
	x = re.sub(r'[^a-zA-Z0-9_]', '', x)
	return x

# identifier for domain x index
def cc_identifier(domain:str ,index: str):
	return f'domain_{strip_domain(domain)}_index_{index}'

############### json utils
# read file
def read_json(filepath: str) -> Dict[str,object]:
    # load
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(e)
        data = {}
    return data


# find domain within warc index files
def get_entries_gen(domain: str, index: str = None) -> List[Dict]:
    '''
    domain = '*.hk01.com'
    index_list = ['2024-26']
    '''
    """
    Generator that returns Common Crawl entries for a search domain as a list of dictionaries. The entries contain the metadata necessary to know where the bytes of records are located. Each entry is a dictionay/json.
    
    The dictionaries have keys:
    'digest', 'filename', 'length', 'mime', 'mime-detected', 'offset', 'status', 'timestamp', 'url', 'urlkey'
    
    Fields have [str] format.
    
    Takes: 
    domain [str] (ex. https://www.nytimes.com/section/politics)
    index_list [iter] (optional) (ex. ['2018-13'])
    
    Returns:
    records [list] 
        {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}

    """
    # where to save requests
    path = f'output/{cc_identifier(domain,index)}.jsonl'
    if os.path.exists(path):
        res = 
    # yield records

    # query index
    cc_url = "http://index.commoncrawl.org/CC-MAIN-%s-index?url=%s&output=json" % (index,quote(domain))
    # https://index.commoncrawl.org/CC-MAIN-2018-13-index?url=*.hk01.com/&output=json
    response = requests.get(cc_url)
    # {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}
    if response.status_code == 200:
        # write to json
        jsonfile = 'output/get_entries_gen.json'
        print(f'[get_entries_gen] save response to {jsonfile}')
        with open(jsonfile, 'w', encoding='utf-8') as f:
            f.write(response.content.decode())
        
        # return one line at a time
        for record in response.content.splitlines():
            yield json.loads(record)
    else:
        # failed for this index
        print("[get_entries_gen][failed]: Code %s / %s" % (response.status_code,cc_url))
'''
get_entries_gen('*.hk01.com',['2024-26'])
'''
#%%

# extract portion of given warc as specified in 'record'
def download_record(record: Dict, return_stream=False) -> str:
    """
    Takes:
    record [dict]
        # example record
        {"urlkey": "com,hk01)/robots.txt", "timestamp": "20180325045021", "url": "https://www.hk01.com/robots.txt", "mime": "text/plain", "mime-detected": "text/plain", "status": "200", "digest": "Q5FVVPDU3XOTE27OJ6E32HYILTR4K62W", "length": "900", "offset": "2786193", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257651820.82/robotstxt/CC-MAIN-20180325044627-20180325064627-00604.warc.gz"}
        {"urlkey": "com,hk01,147marcofu)/", "timestamp": "20180319023357", "url": "http://147marcofu.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "Z6IJ46JXZU7TCLCDINT3OMVFHV5GZPYU", "length": "697", "offset": "19764", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/crawldiagnostics/CC-MAIN-20180319023123-20180319043123-00352.warc.gz"}
        {"urlkey": "com,hk01,147marcofu)/", "timestamp": "20180319023358", "url": "https://147marcofu.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "PCMUQRIXP4CZ46FZZWEG5A4O4Q7NL6IS", "length": "5206", "offset": "524088600", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/warc/CC-MAIN-20180319023123-20180319043123-00127.warc.gz"}
        {"urlkey": "com,hk01,147marcofu)/robots.txt", "timestamp": "20180319023357", "url": "http://147marcofu.hk01.com/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "Z6IJ46JXZU7TCLCDINT3OMVFHV5GZPYU", "length": "709", "offset": "9066", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/robotstxt/CC-MAIN-20180319023123-20180319043123-00299.warc.gz"}
        {"urlkey": "com,hk01,147marcofu)/robots.txt", "timestamp": "20180319023357", "url": "https://147marcofu.hk01.com/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "404", "digest": "PEYIWLDVVYSZO7ECKZVNGGOBO2Z4H753", "length": "1115", "offset": "2259815", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/robotstxt/CC-MAIN-20180319023123-20180319043123-00010.warc.gz"}
        {"urlkey": "com,hk01,2016legcoelection)/", "timestamp": "20180321221615", "url": "http://2016legcoelection.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "DCCZLIYWOISRFWJPYIB2ITVTKDLGMPJD", "length": "10215", "offset": "1349698", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00201.warc.gz"}
        {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}
    
    Returns:
    record [str] (contains cc metadata and html document)
    """
    
    # start and end bytes within WARC file.
    start = int(record['offset'])
    end = start + int(record['length']) -1
    
    # retrieve data (html compressed as gzip)
    s3_url = 'https://commoncrawl.s3.amazonaws.com/'
    '''
    According to the information available, the Common Crawl organization moved their dataset hosting from the commoncrawl.s3.amazonaws.com domain to data.commoncrawl.org sometime in 2021.
    The Common Crawl dataset is a large, publicly available web crawl that is frequently used for natural language processing and other web-based research. The transition to the new data.commoncrawl.org domain was likely done to provide a more stable and reliable hosting solution for the constantly growing dataset.
    However, without access to specific documentation or announcements from the Common Crawl organization, I cannot provide an exact date for when this domain migration occurred. The change likely happened gradually over time as the new hosting infrastructure was brought online and the data was migrated.
    '''
    # https://commoncrawl.org/get-started
    # moved from 'https://commoncrawl.s3.amazonaws.com' to 'https://data.commoncrawl.org'
    s3_url = 'https://data.commoncrawl.org/'
    final_url = s3_url+record['filename']
    # https://data.commoncrawl.org/crawl-data/CC-MAIN-2018-13/segments/1521257651820.82/robotstxt/CC-MAIN-20180325044627-20180325064627-00604.warc.gz
    # https://data.commoncrawl.org/crawl-data/CC-MAIN-2018-13/segments/1521257651820.82/robotstxt/CC-MAIN-20180325044627-20180325064627-00604.wet.gz
    response = requests.get(final_url,headers = {'Range': 'bytes=%i-%i' % (start,end)})
    
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/206
    '''
    The HTTP 206 Partial Content success status response code indicates that the request has succeeded and the body contains the requested ranges of data, as described in the Range header of the request.
    '''
    assert response.status_code == 206
    
    # unzip warc file
    extract = gzip.GzipFile(fileobj=BytesIO(response.content))
    if return_stream:
        return extract # stream
    else:
        return extract.read().decode('utf-8') # string
'''
{"urlkey": "com,hk01)/", "timestamp": "20240614083751", "url": "https://www.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "MJVIRWTZZ3XUMBZV76MYIOXGNXDLB5JP", "length": "65556", "offset": "720642372", "filename": "crawl-data/CC-MAIN-2024-26/segments/1718198861545.42/warc/CC-MAIN-20240614075213-20240614105213-00661.warc.gz", "languages": "zho", "encoding": "UTF-8"}
import json
record = json.loads('{"urlkey": "com,hk01)/", "timestamp": "20240614083751", "url": "https://www.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "MJVIRWTZZ3XUMBZV76MYIOXGNXDLB5JP", "length": "65556", "offset": "720642372", "filename": "crawl-data/CC-MAIN-2024-26/segments/1718198861545.42/warc/CC-MAIN-20240614075213-20240614105213-00661.warc.gz", "languages": "zho", "encoding": "UTF-8"}')
download_record(record)
'''

#%%

def get_records_gen(domain,index_list=None,return_stream=False):
    """
    Generator that returns the Common Crawl records for a particular search domain.

    Takes:
    domain [str]

    Returns:
    record [str]
    """

    entries = get_entries_gen(domain,index_list)
    '''
    # example response
    {"urlkey": "com,hk01)/robots.txt", "timestamp": "20180325045021", "url": "https://www.hk01.com/robots.txt", "mime": "text/plain", "mime-detected": "text/plain", "status": "200", "digest": "Q5FVVPDU3XOTE27OJ6E32HYILTR4K62W", "length": "900", "offset": "2786193", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257651820.82/robotstxt/CC-MAIN-20180325044627-20180325064627-00604.warc.gz"}
    {"urlkey": "com,hk01,147marcofu)/", "timestamp": "20180319023357", "url": "http://147marcofu.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "Z6IJ46JXZU7TCLCDINT3OMVFHV5GZPYU", "length": "697", "offset": "19764", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/crawldiagnostics/CC-MAIN-20180319023123-20180319043123-00352.warc.gz"}
    {"urlkey": "com,hk01,147marcofu)/", "timestamp": "20180319023358", "url": "https://147marcofu.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "PCMUQRIXP4CZ46FZZWEG5A4O4Q7NL6IS", "length": "5206", "offset": "524088600", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/warc/CC-MAIN-20180319023123-20180319043123-00127.warc.gz"}
    {"urlkey": "com,hk01,147marcofu)/robots.txt", "timestamp": "20180319023357", "url": "http://147marcofu.hk01.com/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "Z6IJ46JXZU7TCLCDINT3OMVFHV5GZPYU", "length": "709", "offset": "9066", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/robotstxt/CC-MAIN-20180319023123-20180319043123-00299.warc.gz"}
    {"urlkey": "com,hk01,147marcofu)/robots.txt", "timestamp": "20180319023357", "url": "https://147marcofu.hk01.com/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "404", "digest": "PEYIWLDVVYSZO7ECKZVNGGOBO2Z4H753", "length": "1115", "offset": "2259815", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257646213.26/robotstxt/CC-MAIN-20180319023123-20180319043123-00010.warc.gz"}
    {"urlkey": "com,hk01,2016legcoelection)/", "timestamp": "20180321221615", "url": "http://2016legcoelection.hk01.com/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "DCCZLIYWOISRFWJPYIB2ITVTKDLGMPJD", "length": "10215", "offset": "1349698", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00201.warc.gz"}
    {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}
    '''
    for entry in entries:
        print(entry)
        yield download_record(entry,return_response)
'''
records_gen = get_records_gen('*.hk01.com',['2024-26']) # defines generator
record = next(records_gen) # actually runs it
'''
#%%

# get list of index files
def download_index_list():
    """
    Download the full Common Crawl Index list
    
    Returns:
    index_list [list] (ex. ['2018-13','2018-09',...]
    """
    """ Common Crawl Indices """

    # get list of valid indices
    index_list_url = 'http://index.commoncrawl.org/collinfo.json'
    response = requests.get(index_list_url)

    # write to json
    jsonfile = 'output/download_index_list.json'
    print(f'[download_index_list] save response to {jsonfile}')
    with open(jsonfile, 'w', encoding='utf-8') as f:
        f.write(response.content.decode())
    
    # return list of valid indices
    return [index['id'].replace('CC-MAIN-','') for index in json.loads(response.content.decode('utf-8'))]
'''
download_index_list()
'''

# %%
