#!/usr/bin/python
import sys
import httplib
import json

args = sys.argv[1:]
args += ['']*(4- len(args))

with open('es.json') as data_file:    
    conf = json.load(data_file)

commands = {
	'count': {
		'method': 'GET',
		'uri': '/' + args[1] + '/_count?pretty=true',
		'data': '',
		'code': 'content["count"]'
	},
	'count_exists': {
		'method': 'POST',
		'uri': '/' + args[1] + '/_search?pretty=true',
		'data': '{filter:{exists:{field:"' + args[2] + '"}}}',
		'code': 'content["hits"]["total"]'
	},
	'cluster_health': {
		'method': 'GET',
		'uri': '/_cluster/health?pretty=true',
		'data': ''
	},
	'search_nested_exists': {
		'method': 'POST',
		'uri': '/' + args[1] + '/_search?pretty=true',
		'data': '{filter:{nested:{path:"' + args[2].split('.')[0] + '",filter:{ exists:{field:"' + args[2] + '"}}}}}'
	},
	'count_nested_exists': {
		'method': 'POST',
		'uri': '/' + args[1] + '/_count?pretty=true',
		'data': '{query:{nested:{path:"' + args[2].split('.')[0] + '",filter:{ exists:{field:"' + args[2] + '"}}}}}',
		'code': 'content["count"]'
	},
}

command = commands.get(args[0])
if command == None:
	print 'Command `%s\' not found' % args[0]
	exit(0)	

debug_mode = conf['settings']['debug'] == True

try:
	conn = httplib.HTTPConnection(conf['settings']['host'], conf['settings']['port'])
	
	if debug_mode:
		conn.set_debuglevel(5)

	conn.request(command['method'], command['uri'], command['data'], {"Content-type": "application/json", "Accept": "text/plain"})
	
	r = conn.getresponse()
	if r.status != 200:
		print r.reason
		exit(0)
		
	if command.get('code') == None:
		print r.read()
	else:
		content = json.loads(r.read())
		print eval(command['code'])
except Exception, e:
	print 'Error: ' + str(e)
	exit(1)
