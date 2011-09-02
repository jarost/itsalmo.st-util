#! /usr/bin/env python2.7

import sys, os, subprocess, datetime, gzip, tarfile, shutil

sys.path.append("./lib")
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3CreateError, S3DataError, S3PermissionsError, S3ResponseError

backup_path = '/home/ubuntu/backups'
#backup_path = '/Users/andrewmahon/backups/'

host_name = 'tc-dev'
#db_name = 'its_almost'
db_name = None

def main():
  print '\nMongoDB Backup Helper\n\n'
  
  #internal
  files_to_cleanup = []

  print 'Start:   Dumping MongoDB'
  try:
    os.chdir(backup_path)
    if db_name:
      subprocess.call(['mongodump','-d',db_name])
    else:
      subprocess.call(['mongodump'])
    files_to_cleanup.append('dump')
  except:
    print 'Unexpected error with mongodump'
  print 'Success: Dumping MongoDB\n'

  print 'Start:   Creating Tar'
  try:
    now = datetime.datetime.now()
    if db_name:
      filename = host_name + '.mongo.'+str(db_name)+'.'+str(now.month)+'.'+str(now.day)+'.'+str(now.year)+'.'+str(now.hour)+'.'+str(now.minute)+'.'+str(now.second)+'.bak.tar'
    else:
      filename = host_name + '.mongo.'+str(now.month)+'.'+str(now.day)+'.'+str(now.year)+'.'+str(now.hour)+'.'+str(now.minute)+'.'+str(now.second)+'.bak.tar'
    tar = tarfile.open(filename, "w")
    #tar.add(backup_path+'dump')
    tar.add('dump')
    tar.close()
    files_to_cleanup.append(''+filename)
  except:
    print 'Unexpected error with tar file creation'
  print 'Success: Creating Tar\n'

  print 'Start:   Gzipping Tar'
  try:
    f_in = open(filename, 'rb')
    filename = filename + '.gz'
    f_out = gzip.open(filename, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
  except:
    print 'Unexpected error with tar file gzipping'
  print 'Success: Gzipping Tar\n'
  
  print 'Start:   Sending to S3'
  if not upload_to_s3(filename,'tc-dev-mongo',filename):
    print 'Error:   Sending to S3\n'
  else:
    print 'Success: Sending to S3\n'
  
  print 'Start:   Cleaning Up'
  for f in files_to_cleanup:
    try:
      os.remove(f)
      print 'Removed file \'' + f +'\''
    except OSError:
      shutil.rmtree(f)
      print 'Removed dir \'' + f +'\''
  print 'Success: Cleaning Up\n'

def upload_to_s3(source_filename, target_bucket, target_key):
  def s3_upload_status_callback(uploaded, total):
    print str(100*(float(uploaded)) / (float(total))) + '% uploaded' + '(' + str(uploaded) + '/' + str(total) + ')'
  
  if not os.path.exists(source_filename):
    print 'File with name \'' + source_filename + '\' does not exist.'
    print 'Exiting.'
    return False
    
  try:
    c = boto.connect_s3()
    print 'Opening bucket \'' + target_bucket + '\'.'
    b = c.get_bucket(target_bucket)
    if not b:
      print 'Bucket \'' + target_bucket + '\' does not exist. Creating.'
      b = c.create_bucket(target_bucket)
    k = Key(b)
    print 'Checking for object with key \'' + target_key + '\'.'
    k.key = target_key
    if k.exists():
      print 'Object with key \'' + target_key + '\' already in bucket \'' + target_bucket + '\'.'
      print 'Exiting.'
      return False
    print 'Creating object with key \'' + target_key + '\' from file \'' + source_filename + '\'.'
    k.set_contents_from_filename(source_filename, cb=s3_upload_status_callback, num_cb=10)
  except S3CreateError:
    print 'Unexpected error with S3 Create.'
    return False
  except S3DataError:
    print 'Unexpected error with S3 Data.'
    return False
  except:
    print 'Unexpected error with S3'
    return False
  return True

main()