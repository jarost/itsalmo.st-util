#! /usr/bin/env python2.7

import sys, os, subprocess, datetime, gzip, tarfile

sys.path.append("lib")
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3CreateError, S3DataError, S3PermissionsError, S3ResponseError

source_filename = 'fentol_lake.jpg'
target_bucket = 'tc-util-test'
target_key = 'upload'

def main():
  def s3_upload_status_callback(uploaded, total):
    print str(100*(float(uploaded)) / (float(total))) + '% uploaded' + '(' + str(uploaded) + '/' + str(total) + ')'
  
  if not os.path.exists(source_filename):
    print 'File with name \'' + source_filename + '\' does not exist.'
    print 'Exiting.'
    return
    
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
      return
    print 'Creating object with key \'' + target_key + '\' from file \'' + source_filename + '\'.'
    k.set_contents_from_filename(source_filename, cb=s3_upload_status_callback, num_cb=10)
  except S3CreateError:
    print 'Unexpected error with S3 Create.'
  except S3DataError:
    print 'Unexpected error with S3 Data.'
  except:
    print 'Unexpected error with S3'

main()