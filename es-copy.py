
#!/usr/bin/env python
# original code from http://codereview.stackexchange.com/q/56979
# modified by Felipe Pena <felipensp at gmail dot com>

from __future__ import division
from argparse import ArgumentParser, FileType
import json
import sys
import requests
from time import sleep


class ElasticSearch():
    def __init__(self, url):
        self.url = url

    def request(self, method, path, data=None):
        return (requests.request(
                method, 'http://%s/%s' % (self.url, path),
                data=data,
                headers={'Content-type': 'application/json'}).json())

    def post(self, path, data):
        return self.request('post', path, data)
        
    def put(self, path, data):
        return self.request('put', path, data)

    def get(self, path, data=None):
        return self.request('get', path, data)

    def scan_and_scroll(self, index):
        response = self.get('%s/_search?search_type=scan&scroll=1m' % index,
                            data=json.dumps({"query": {"match_all": {}},
                                             "size": 1000, "fields": ["*", "_source"]}))
        while True:
            response = self.get('_search/scroll?scroll=2m',
                                data=response['_scroll_id'])
            if len(response['hits']['hits']) == 0:
                return
            yield response['hits']['hits']

    def set_mapping(self, index, mappings):
        return self.put(index+'/_mapping', data=json.dumps(mappings))
        
    def get_mapping(self, index):
        return json.dumps(self.get(index+'/_mapping')[index.split('/')[0]]['mappings'][index.split('/')[1]])

    def count(self, index):
        response = self.get('%s/_search' % index)
        return response['hits']['total'] if 'hits' in response else 0

    def bulk_insert(self, index, bulk):	
		newdata = ''
		for line in bulk:
			metadata = {'index': {'_index': index.split('/')[0], 
				'_type': index.split('/')[1],
				'_id': line['_id']}}
				
			if 'fields' in line and '_parent' in line['fields']:
				metadata['index']['_parent'] = line['fields']['_parent']
				
			newdata += json.dumps(metadata) + "\n"
			"""
			if 'costCenter' in line['_source']:
				del line['_source']['costCenter']
			if 'code' in line['_source']:
				del line['_source']['code']
			line['_source']['segments'] = []
			"""		
			newdata += json.dumps(line['_source']) + "\n"
                         
		return self.post('_bulk', data=newdata)

    def bulk_delete(self, index, bulk):
		"""
        return self.post('_bulk',
                         data=''.join(
                         json.dumps({'index': {'_index': index.split('/')[0],
                                                '_type': index.split('/')[1],
                                                '_id': line2['number'],
                                                '_parent': line['_id']}}) +
                         "\n" +
                         json.dumps(line2) + "\n" for line in bulk for line2 in line['_source']['contracts']))
		"""
		
		newdata = ''
		for line in bulk:
			"""
			if line['fields']['_parent'].find('_') != -1:
				continue
			print line['fields']['_parent']
			"""
			newdata += json.dumps({'delete': {'_index': index.split('/')[0],
                                                '_type': index.split('/')[1],
                                                '_id': line['_id']}}) + "\n"				
                         
		return self.post('_bulk', data=newdata)

    def drop(self, index):
        return self.request('delete', index)

    def alias(self, index, to):
        return self.request('put', '%s/_alias/%s' % (index, to))


def change_mapping_and_reindex(elasticsearch, mapping_file, index):
    es = ElasticSearch(elasticsearch)

    mapping_text = mapping_file.read()
    temporary_index = None
    for i in range(10):
        try_temporary_index = index + '-tmp-' + str(i)
        print "Setting mapping to %s" % try_temporary_index
        response = es.set_mapping(try_temporary_index,
                                  json.loads(mapping_text))
                                  
        if 'acknowledged' in response and response['acknowledged']:
            temporary_index = try_temporary_index
            break
    if temporary_index is None:
        print "Can't find a temporary index to work with."
        return False

    old_index_count = es.count(index)
    new_index_count = es.count(temporary_index)
    print 'old index count: ' + str(old_index_count)
    print 'new index count: ' + str(new_index_count)

    print 'starting bulk insert into ' + temporary_index
    for bulk in es.scan_and_scroll(index):
        es.bulk_insert(temporary_index, bulk)
        new_index_count = es.count(temporary_index)
        percent = 100 * new_index_count / old_index_count
        print ("\r%.2f%%" + 10 * " ") % percent,
    print "\nDone"

    for i in range(1000):
        new_index_count = es.count(temporary_index)
        if new_index_count == old_index_count:
            print "OK, same number of raws in both index."
            break
        print ("Not the same number of raws in old and new... "
               "waiting a bit..."
               "(old=%d, new=%d)" % (old_index_count, new_index_count))
        sleep(4)
	
    #print "Deleting %s" % index
    #es.drop(index)
    #print "Aliasing %s to %s" % (temporary_index, index)
    #es.alias(temporary_index, index)
    
def copy_index_doc(addr_in, mapping_file, to_doc, from_doc, addr_out):
    es = ElasticSearch(addr_in)
    es_out = es if addr_out == None or addr_in == addr_out else ElasticSearch(addr_out)
        
    """
    print "Setting mapping to %s" % to_doc
    if mapping_file == None:
        mapping_text = es.get_mapping(from_doc)
    else:
        mapping_text = mapping_file.read()
	
		
    response = es_out.set_mapping(to_doc, json.loads(mapping_text))
    if not 'acknowledged' in response:
        print 'Cannot create mapping to index %s (%s)' % (to_doc, response)
        return
	"""
    old_index_count = es.count(from_doc)
    new_index_count = es_out.count(to_doc)
    print 'old index count: ' + str(old_index_count)
    print 'new index count: ' + str(new_index_count)

    print 'starting bulk insert into ' + to_doc
    for bulk in es.scan_and_scroll(from_doc):
        sys.stdout.flush()
        res = es_out.bulk_insert(to_doc, bulk)
        if 'error' in res or ('errors' in res and res['errors'] != False):
			print res
        new_index_count = es_out.count(to_doc)
        percent = 100 * new_index_count / old_index_count
        print ("\r%.2f%%" + 10 * " ") % percent,
    print "\nDone"

    for i in range(1000):
        new_index_count = es_out.count(to_doc)
        if new_index_count == old_index_count:
            print "OK, same number of rows in both index."
            break
        print ("Not the same number of raws in old and new... "
               "waiting a bit..."
               "(old=%d, new=%d)" % (old_index_count, new_index_count))
        sleep(30)


def delete_docs(elasticsearch, from_doc):
    es = ElasticSearch(elasticsearch)
    for bulk in es.scan_and_scroll(from_doc):
        sys.stdout.flush()
        res = es.bulk_delete(from_doc, bulk)
        if 'error' in res or ('errors' in res and res['errors'] != False):
			print res
    print "\nDone"
    
if __name__ == '__main__':
    parser = ArgumentParser(
        description="Remap and reindex the given index, but only if you stoped"
        "writing to it (will fail if you're writing")
    parser.add_argument('--delete', help='bulk delete', action='store_true')
    parser.add_argument('--index', help='index to remap')
    parser.add_argument('--to', help='doc to load')
    parser.add_argument('--input', help='ES host input')
    parser.add_argument('--output', help='ES host output')
    parser.add_argument('--mapping',
                        help='Mapping file, starts with {"mappings"...',
                        type=FileType('r'))
    args = parser.parse_args()
    
    if args.delete != None:
		delete_docs(args.input, args.index)
    if args.to == None:
        change_mapping_and_reindex(args.input, args.mapping, args.index)
    else:
		copy_index_doc(args.input, args.mapping, args.to, args.index, args.output)
		
