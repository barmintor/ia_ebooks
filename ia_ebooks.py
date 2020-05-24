from sys import stderr
from io import BytesIO
import json
import re
from time import sleep
# library dependencies
import requests
from pymarc import MARCReader, Record
from pymarc.exceptions import RecordLengthInvalid

CLIO_DERIVED_ID = re.compile('^ldpd[_]+([0-9A-Za-z]+)[_]+\\d+$')
CLIO_LINK = re.compile('"http:\\/\\/clio.columbia.edu\\/catalog\\/([0-9A-Za-z]+)"')

def query_internet_archive(queries, page_size=50, page=1):
	q = ' AND '.join(["%s:(%s)" % query for query in queries])
	params = {
		'q': q,
		'callback': '',
		'rows': page_size,
		'page': page,
		'output': 'json',
		'sort[]': '__sort desc'
	}
	response = requests.get("https://archive.org/advancedsearch.php", params=params)
	return json.loads(response.text)

class IA:
	"""
	An iterable class to prevent storing many large IA requests in memory
	"""
	def __init__(self, queries, page_size=50, page=1):
		self.params = {
			'q': ' AND '.join(["%s:(%s)" % query for query in queries]),
			'callback': '',
			'rows': page_size,
			'output': 'json',
			'sort[]': '__sort desc',
			'page': (page - 1) # will iterate when first page is fetched
		}
		self.docs = []
		self.more = True
	def __fetch_next_page__(self):
		self.params.update({ 'page': (self.params['page'] + 1) })
		response_body = requests.get("https://archive.org/advancedsearch.php", params=self.params).text
		response = json.loads(response_body)
		self.docs = response['response']['docs']
		numFound = int(response['response']['numFound'])
		self.more = numFound > (self.params['rows'] * self.params['page'])
	def __iter__(self):
		self.docs = []
		self.more = True
		return self
	def __next__(self):
		if len(self.docs) == 0 and self.more:
			self.__fetch_next_page__()
		if len(self.docs) == 0:
			raise StopIteration
		return self.docs.pop(0)



def fetch_iter(collection='ColumbiaUniversityLibraries', mediatype='collection', page_size=50):
	"""
	get an iterator for the matching documents

	:param collection: the IA collection id
	:param mediatype: collection or texts
	:param page_size
	:returns: an iterator of IA results
	"""
	queries = [
		('collection', collection),
		('mediatype', mediatype)
	]
	return iter(IA(queries, page_size))

def fetch_list(collection='ColumbiaUniversityLibraries', mediatype='collection', page_size=50):
	"""
	get all the matching documents, intermediate pages of page_size fetched.

	:param collection: the IA collection id
	:param mediatype: collection or texts
	:param page_size
	:returns: a list of IA results
	"""
	queries = [
		('collection', collection),
		('mediatype', mediatype)
	]
	page = 0
	numFound = 1
	docs = []
	while numFound > (page_size * page):
		page += 1
		response = query_internet_archive(queries, page_size, page)
		numFound = int(response['response']['numFound'])
		docs.extend(response['response']['docs'])
	return docs

def fetch_ebooks(collection='ColumbiaUniversityLibraries', page_size=50):
	"""
	get all the ebooks in a collection, intermediate pages of page_size fetched.

	:param collection: the containing IA collection id
	:param page_size
	:returns: an iterator of IA ebook documents
	"""
	return fetch_iter(collection, 'texts', page_size)

def fetch_collections(collection='ColumbiaUniversityLibraries', page_size=50):
	"""
	get all the collections in a collection, intermediate pages of page_size fetched.

	:param collection: the containing IA collection id
	:param page_size
	:returns: an iterator of IA collection documents
	"""
	return fetch_iter(collection, 'collection', page_size)

def fetch_document(identifier):
	"""
	get a single document from IA by id

	:param identifier: the IA document id
	:returns: an IA document
	"""
	queries = [
		('identifier', identifier)
	]
	response = query_internet_archive(queries, 1, 1)
	numFound = int(response['response']['numFound'])
	if numFound > 0:
		return response['response']['docs'][0]
	else:
		return None

def clio_id(doc):
	"""
	inspect an IA document for its apparent CLIO bib id

	:param doc: the IA document
	:returns: a String CLIO bib id
	"""
	bib_id = None
	id_match = CLIO_DERIVED_ID.match(doc['identifier'])
	if id_match:
		bib_id = id_match.group(1)
	if bib_id is None:
		link_match = CLIO_LINK.search(doc.get('stripped_tags',''))
		if link_match:
			bib_id = link_match.group(1)
	return bib_id

def fetch_clio(identifier, retry_after=-1):
	"""
	get a single document from CLIO by id

	:param identifier: the CLIO bib id
	:param retry_after: seconds to sleep before fetching
	:returns: a pymarc.Record
	"""
	if retry_after != -1: sleep(retry_after) # we love you CLIO
	response = requests.get("https://clio.columbia.edu/catalog/%s.marc" % identifier)
	marc_reader = MARCReader(BytesIO(response.content))
	try:
		return next(marc_reader)
	except (ValueError, RecordLengthInvalid):
		if (retry_after == -1) and (response.status_code == 429):
			retry_after = int(response.headers['Retry-After']) + 1
			print(("CLIO rate limiting, waiting %s: %s" % (retry_after, response.url)), file=stderr)
			print(json.dumps(dict(response.headers)), file=stderr)
			return fetch_clio(identifier, retry_after)
		else:
			print(("Collegially retrying only once: %s" % response.url), file=stderr)
			return Record()

def ia_links(doc):
	"""
	inspect an IA document for its apparent CLIO bib id

	:param doc: the IA document
	:returns: a dictionary of links
	"""
	ia_id = doc['identifier']
	return {
		'thumbnail': ("https://archive.org/services/img/%s" % ia_id),
		'poster': ("https://archive.org/download/%s/page/cover_medium.jpg" % ia_id),
		'pdf': ("https://archive.org/download/%s/%s.pdf" % (ia_id, ia_id)),
		'iframe': ("https://archive.org/stream/%s?ui=full&showNavbar=false" % ia_id)
	}

def dump_iterable(docs):
	"""
	prints an iterable

	:param docs: the iterator of documents
	"""
	# print the containing brackets in anticipation of large iterable
	print('[')
	doc = next(docs, None)
	if doc is not None:
		print(json.dumps(doc, indent=2))
		doc = next(docs, None)
		while doc is not None:
			print(',')
			print(json.dumps(doc, indent=2))
			doc = next(docs, None)
	print(']')

def help(command=None):
	if command is not None: print("python ia_ebooks.py %s" % command)
	print('usage: python ia_ebooks.py [cmd [params...]]')
	print('cmd "list-collections": list all collection id\'s and descriptions. id\'s can be used as collection param value')
	print('cmd "collection": list all ebooks in a collection.')
	print('cmd "ebook": get one ebook by identifier.')

if __name__ == "__main__":
	import sys
	import argparse

	parser = argparse.ArgumentParser(description='Fetch Internet Archive Ebooks.')
	parser.add_argument('command', nargs='?', default='help', help='the fetch command: list-collections, list-ebooks, ebook, clio')
	parser.add_argument('identifier', nargs='?', default=None, help='the document identifier to fetch (IA or CLIO per command)')
	parser.add_argument('-C', '--collection', help='collection to query', default='ColumbiaUniversityLibraries')
	parser.add_argument('-F', '--format', help='how to display data: json (default) or tsv (of identifiers)', default='json')
	parser.add_argument('--clio', help='add clio data', action="store_true", default=False)
	args = parser.parse_args()
	if args.command == "list-collections":
		if args.format == 'json':
			# use a generator to lazily map to identifier and description keys
			dump_iterable(({'identifier':doc['identifier'], 'description': doc['description'] } for doc in fetch_collections(args.collection)))
		else:
			print("identifier\tdescription")
			for doc in fetch_collections(args.collection):
				print("%s\t%s" % (doc['identifier'], doc['description']))
	elif args.command == "list-ebooks":
		if args.identifier is not None:
			parser.error('use the collection flag "-C" to scope ebook list to a collection')
		if args.format == 'json':
			docs = fetch_ebooks(args.collection, 100)
			if args.clio:
				# use a generator to lazily add clio data
				dump_iterable(({'clio': fetch_clio(clio_id(doc)).as_dict(), **ia_links(doc), **doc} for doc in docs))
			else:
				dump_iterable({**doc, 'links': ia_links(doc)} for doc in docs)
		else:
			print("identifier\tclio_id")
			for doc in fetch_ebooks("muslim-world-manuscripts", 100):
				print("%s\t%s" % (doc['identifier'], clio_id(doc)))
	elif args.command == "ebook":
		if args.identifier is None:
			parser.error('an identifier is required to fetch a single document.')
		doc = fetch_document(args.identifier)
		if args.format == 'json':
			if args.clio:
				json.dumps({**doc, 'clio': fetch_clio(clio_id(doc)).as_dict(), 'links': ia_links(doc)}, indent=2)
			else:
				json.dumps({**doc, 'links': ia_links(doc)}, indent=2)
		else:
			print("identifier\tclio_id")
			print("%s\t%s" % (doc['identifier'], clio_id(doc)))
	elif args.command == "clio":
		if args.identifier is None:
			parser.error('an identifier is required to fetch a single document.')
		clio_record = fetch_clio(args.identifier)
		print(clio_record.as_json(indent=2))
	else:
		parser.print_help()
