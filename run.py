#%%
from src.get_records import get_records_gen # search common crawl index files and return generator for machine records
from src.get_html import extract_html as extract_warc_content # strip content from warc files
from trafilatura import extract as extract_html_content # strip content from html files
from warcio.archiveiterator import ArchiveIterator
import re
import hashlib
from typing import List
#%%

############################################################
# RECORD STRING
############################################################
records_gen = get_records_gen(domain = '*.hk01.com',index_list = ['2024-26']) # build record generator (returns string)
record_str = next(records_gen) # get record_str
#print(type(record_str),end='\n\n\n') # record_str is a string

#%%
'''
# inspect record_str
print(record_str[:500],end='\n\n\n') # at the start of the string you get the WARC metadata
print(record_str[-500:],end='\n\n\n') # at the end of the string you get your html
print(record_str,end='\n\n\n') # %%
'''

# extract stuff
warc_content = extract_warc_content(record_str) # extract html
html_content = extract_html_content(warc_content) # extract content
print(warc_content)
print(html_content)

#%%
############################################################
# RECORD STREAM
############################################################
records_gen_stream = get_records_gen(domain = '*.hk01.com',index_list = ['2024-26'], return_stream=True) # build record generator (returns stream)
record_stream = next(records_gen_stream) # get record stream
record = next(ArchiveIterator(record_stream)) # parsed record with warcio


#%%
def extract_record(record: ArchiveIterator):
	if record.rec_type == 'response':
		record_header = record.http_headers
		record_content = record.content_stream().read().decode('utf-8')
		record_html = extract_html_content(record_content)
		record_hash = hashlib.md5(str(record_header).encode()).hexdigest()
		return record_html,record_hash
	else:
		return None




# strip_special_chars
def clean_domain(domain:str) -> str:
	cleaned_domain = domain.replace('*.','')
	cleaned_domain = cleaned_domain.replace('.','_')
	cleaned_domain = re.sub(r'[^a-zA-Z0-9_]', '', cleaned_domain)
	return cleaned_domain

# build identifier to save files
def common_crawl_job_identifier(domain:str ,index_list: List[str]):

	# concat with index_list
	return f"domain_{cleaned_domain}_index_{'_'.join(index_list)}"
'''
common_crawl_job_identifier(domain = '*.hk01.com',index_list = ['2024-26'])
'''
	


import os
THREAD_COMMENTS_JSONL=''

# handle record
def handle_record(domain: str, index: str, record: ArchiveIterator) -> None:
	# build hash

def handle_record(cc_identifier: str, record: ArchiveIterator):
	# try to extract record
	res = extract_record(record)
	if res is None:
		return

	# extract successful
	record_html,record_hash = res



	# write to json
	jsonfile = f'output/{cc_identifier}_{record_hash}.json'
	print(f'[download_index_list] save response to {jsonfile}')
	with open(jsonfile, 'a', encoding='utf-8') as f:
		cc_identifier = common_crawl_job_identifier(domain = '*.hk01.com',index_list = ['2024-26'])
		f.write(response.content.decode())

# write record
def write_comments_to_jsonl(domain: str, index: str, record_hash: str, record_content: str) -> None:
	# dirname
	dirname = os.path.dirname(domain)
	if not os.path.exists(dirname): os.makedirs(dirname)

	# filename
	filename = common_crawl_job_identifier(domain,[index]) + '.json'

	# filepath
	filepath = os.path.join(dirname,filename)

    with lock:
        dico = {
            'record_hash' : record_hash, # hash of record header
            'record_content' : record_content, # record contents
        }
        # write line for each record
        with open(filepath, 'a', encoding='utf-8') as f:
			#print(f'[write_comments_to_jsonl] {generic_dico}')
			line = json.dumps(dico, ensure_ascii=False) # build line
			#print(f'[write_comments_to_jsonl] {line}')
			f.write(line + '\n') # write line




'''
https://commoncrawl.org/blog/web-archiving-file-formats-explained

# arc 	# old-school 	# https://gist.githubusercontent.com/pjox/4de9fb8962b715fe73bbe09d70b2e94e/raw/dd02d05261b6a62c87d6bc5a4ecd2a6dc2d9a2f4/swindon.arc
# warc 	# newer		 	# https://gist.github.com/pjox/4de9fb8962b715fe73bbe09d70b2e94e?h=1#file-saturn-warc
# wat 	# meta-data		# https://gist.github.com/pjox/4de9fb8962b715fe73bbe09d70b2e94e?h=1#file-saturn-pretty-wat
# wet 	# text-only		# https://gist.githubusercontent.com/pjox/4de9fb8962b715fe73bbe09d70b2e94e/raw/9cde1229cf9cd18b12a6c87ad43f36f7bdc996c4/saturn.wet


WARC files which store the raw crawl data
WAT files which store computed metadata for the data stored in the WARC
WET files which store extracted plaintext from the data stored in the WARC
'''
'''
import warc

for num, record in enumerate(f):
	if record['WARC-Type'] == 'response':
		# Imagine we're interested in the URL, the length of content, and any Content-Type strings in there
		print record['WARC-Target-URI'], record['Content-Length']
		print record.payload.read()
		print '=-=-' * 10
	if num > 100:
		break

'''
# %%


# full list
index_list = download_index_list()

