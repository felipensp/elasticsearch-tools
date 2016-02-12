#!/usr/bin/python

import subprocess
import os
import sys

counts = [
	['caterpillar/part'],
	['caterpillar/part', 'count_nested_exists', 'info.number'],
	['caterpillar/jobcode'],
	['sotreq/client'],
	['sotreq/equipment'],
	['sotreq/equipment', 'count_nested_exists', 'contracts.serialNumber'],
	['sotreq/equipment', 'count_exists', 'serviceOrders.number'],
	['sotreq/equipment', 'count_exists', 'serviceLetters.code'],
	['caterpillar/componentcode'],
	['caterpillar/contracttype'],
	['sotreq/billet'],
	['sotreq/standardjob'],
	['sotreq/invoice'],
]

for index in counts:	
	if len(index) > 1:
		print ('%-30s' % (index[0].split('/')[1] + '/' + index[2].split('.')[0])) + ':\t', subprocess.check_output(['./es.py', index[1], index[0], index[2]]),
	else:
		print ('%-30s' % index[0].split('/')[1]) + ':\t', subprocess.check_output(['./es.py', 'count', index[0]]),
