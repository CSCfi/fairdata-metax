# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import ssl
import time
from urllib import request

d = print
d("STARTING MEASUREMENTS")

url = 'https://metax.csc.local/rest/files/urn:nbn:fi:csc-ida201401200000100000'
# url = 'https://metax.csc.local/rest/files/d1a53535-2adb-5184-a10b-8dd7338d0a41'
times = []
run_times = 20

# deal with self-signed certificate
myssl = ssl.create_default_context()
myssl.check_hostname = False
myssl.verify_mode = ssl.CERT_NONE

for i in range(1, run_times + 1):
    start = time.time()
    res = request.urlopen(url, context=myssl)
    end = time.time()
    time_elapsed = end - start
    d("Elapsed time: %.6f seconds" % time_elapsed)
    times.append(time_elapsed)

d("Ran %d times, average time: %.6f seconds" % (run_times, sum(times) / float(len(times))))
