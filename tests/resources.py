#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  resources.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  ./tests/resources.py
#
#######################################################################

S3_MOCK_EVENT = '''{  
   "Records":[
      {  
         "eventVersion":"0.1",
         "eventSource":"viaa:s3",
         "eventTime":"2019-12-12T00:39:15.049Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"Object Owner CN+OR-id"
         },
         "requestParameters":{  
            "sourceIPAddress":"54.93.243.153"
         },
         "responseElements":{  
            "x-viaa-request-id":"bb90f87be822889b21f197b43090cb4a"
         },
         "s3":{
            "domain":{
               "name":"s3"
            },
            "bucket":{
               "name":"MAM_HighresVideo",
               "ownerIdentity":{
                  "principalId":"Bucket Owner CN+OR-id"
               },
               "metadata":{
                  "tenant": "OR-rf5kf25",
                  "kind":"ingest"
               }
            },
            "object":{  
               "key":"191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
               "size":4248725,
               "eTag":"77930bf06b236e089a22a255e6b28377",
               "metadata":{
                  "Type": "application/mxf",
                  "Castor-System-Cid":"7da76343ad6bc9f2f739f0595a2756e4",
                  "Content-Md5":"rbyRpD6YijtbdFuFKakLYQ==",
                  "x-md5sum-meta": "7ef01fd710fec9a175d28c4a31dc49a2"
               }
            }
         }
      }
   ]
}'''

METADATA_DICT = {
  "Dynamic": {
    "s3_object_key": "191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF",
    "s3_bucket": "MAM_HighresVideo",
    "PID": "a1b2c3d4e5"
  },
  "Technical": {
    "Md5": "3d71f15f2d9ed0ef94d7fdc525c95e15"
  }
}

# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
