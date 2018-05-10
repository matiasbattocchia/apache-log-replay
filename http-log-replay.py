"""Replay requests from an HTTP access log file.

- Takes time between requests into account, with option to speed up the replay.
- Allows one to send all requests to a selected server (proxy).
"""

import sys
import time
import urllib.request
from datetime import datetime
from optparse import OptionParser
from operator import itemgetter
import re

# Default log format
NGINX = '(?P<remote_addr>.+?) - (?P<remote_user>.+?) \[(?P<time_local>.+?)\] "GET (?P<url>.+?) HTTP/1.1" (?P<status>.+?) (?P<body_bytes_sent>.+?) "(?P<http_referer>.+?)" "(?P<http_user_agent>.+?)"'

regexp = re.compile(NGINX)

TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

def main(filename, proxy, speedup=1):
    """Setup and start replaying."""
    requests = parse_logfile(filename)
    setup_http_client(proxy)
    replay(requests, speedup)

def replay(requests, speedup):
    """Replay the requests passed as argument"""
    # time sort requests
    requests = sorted(requests, key=itemgetter('time'))

    total_delta = requests[-1]['time'] - requests[0]['time']

    print("%d requests to go (time: %s)" % (len(requests), total_delta))

    last_time = requests[0]['time']

    for request in requests:
        time_delta = (request['time'] - last_time) // speedup

        if time_delta:
            if time_delta.seconds > 10:
                print("(next request in %d seconds)" % time_delta.seconds)
            time.sleep(time_delta.seconds)

        last_time = request['time']

        try:
            req_result = "OK"
            urllib.request.urlopen(request['url'])
        except Exception:
            req_result = "FAILED"
        print("[%s] REQUEST: %s -- %s"
            % (request['time'].strftime("%H:%M:%S"), request['url'], req_result))

def setup_http_client(proxy):
    """Configure proxy server and install HTTP opener"""
    proxy_config = {'http': proxy} if proxy else {}
    proxy_handler = urllib.request.ProxyHandler(proxy_config)
    opener = urllib.request.build_opener(proxy_handler)
    urllib.request.install_opener(opener)

def parse_logfile(filename):
    """Parse the logfile and return a list with tuples of the form
    (<request time>, <requested host>, <requested url>)
    """
    logfile = open(filename, "r")
    requests = []

    for line in logfile:
        match = regexp.match(line)

        if match:
            requests.append({
                'time' : datetime.strptime(match.group('time_local'), TIME_FORMAT),
                'url'  : match.group('url')
            })
        else:
            print('parse error: ' + line)

    if not requests:
        print("Seems like I don't know how to parse this file!")
    return requests

if __name__ == "__main__":
    """Parse command line options."""
    usage = "usage: %prog [options] logfile"
    parser = OptionParser(usage)
    parser.add_option('-p', '--proxy',
        help='send requests to server PROXY',
        dest='proxy',
        default=None)
    parser.add_option('-s', '--speedup',
        help='make time run faster by factor SPEEDUP',
        dest='speedup',
        type='int',
        default=1)
    (options, args) = parser.parse_args()
    if len(args) == 1:
        main(args[0], options.proxy, options.speedup)
    else:
        parser.error("incorrect number of arguments")

