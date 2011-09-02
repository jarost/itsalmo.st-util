#! /usr/bin/env python2.7

import sys, os, subprocess, datetime, gzip, tarfile

sys.path.append("lib")
import boto

#backup_path = '/home/ubuntu/backups'
backup_path = '/Users/andrewmahon/backups/'

#db_name = 'tc-dev'
db_name = 'awm-local'

#internal
files_to_cleanup = []

#subprocess.call('cd '+backup_path+' | mongodump')
os.chdir(backup_path)
subprocess.call('mongodump')
files_to_cleanup.append('dump')

now = datetime.datetime.now()

filename = db_name + '.mongo.'+str(now.month)+'.'+str(now.day)+'.'+str(now.year)+'.bak.tar'

tar = tarfile.open(filename, "w")
tar.add(backup_path+'dump')
tar.close()
#we will leave compressed version on server machine
#files_to_cleanup.append(''+filename)

f_in = open(filename, 'rb')
filename = filename + '.gz'
f_out = gzip.open(filename, 'wb')
f_out.writelines(f_in)
f_out.close()
f_in.close()
files_to_cleanup.append(''+filename)


print files_to_cleanup






