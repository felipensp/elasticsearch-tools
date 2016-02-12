#!/usr/bin/python

import sys
import datetime
import shutil
import subprocess
import tarfile

ELASTICSEARCH_DIR = '/home/ec2-user/elasticsearch-1.6.1'
MAIN_BACKUP_DIRECTORY = '/home/ec2-user/backups/elasticsearch_{0}'
BACKUP_LOG_FILE = '/home/ec2-user/backups/out.log'

sys.stdout = sys.stderr = open(BACKUP_LOG_FILE, 'a') 

def copy_files(dstdir):
	shutil.copytree(ELASTICSEARCH_DIR, dstdir)    
    
def get_backup_directory(base_directory):
	date = str(datetime.datetime.now())[:16]
	date = date.replace(' ', '_').replace(':', '')
	return base_directory.format(date)
	
def compress_backup(backup_directory):
	tar = tarfile.open("%s.tar.gz" % backup_directory, "w:gz")
	tar.add(backup_directory)
	tar.close()
		
def remove_backup_directory(backup_directory):
	shutil.rmtree(backup_directory)	
		
def start_service():
	ret = subprocess.call(["/usr/sbin/service", "elasticsearch", "start"], stdout=sys.stdout, stderr=sys.stderr) 
	if ret != 0:
		print 'failed to start elasticsearch'
		exit(1)
	
def stop_service():
	ret = subprocess.call(["/usr/sbin/service", "elasticsearch", "stop"], stdout=sys.stdout, stderr=sys.stderr) 
	if ret != 0:
		print 'failed to stop elasticsearch'
		exit(1)
		
def perform_backup(base_directory):
	print 'Start backup - %s' % str(datetime.datetime.now())
	sys.stdout.flush()
	try:		
		stop_service()
		backup_directory = get_backup_directory(base_directory)
		copy_files(backup_directory)
		start_service()
		compress_backup(backup_directory)
		remove_backup_directory(backup_directory)
	finally:		
		print 'End backup - %s' % str(datetime.datetime.now())
		sys.stdout.flush()

def main():	
	perform_backup(MAIN_BACKUP_DIRECTORY)		
		
if __name__ == '__main__':
    main()
