import sys
import helpers
import time

import os

import argparse

parser = argparse.ArgumentParser(description='Replicate data via redis.')
parser.add_argument('-c',
    action="store", dest="configFileName",
    help='Name of configfile (for example localhost.json)',
    default="config/localhost.json")

parser.add_argument('-d',
    action="store", dest="pmdDataFolder",
    help='Name of directory to watch.',
    default="pmddata")

parser.add_argument('-s',
    action="store", dest="sleeptime", type=float,
    help='Time for polling updates.',
    default=1.0)

results = parser.parse_args()
print results

r = helpers.connect_db(results.configFileName)

if not r:
    sys.exit(1)


# helpers.download_all(r, 'cache')
if results.pmdDataFolder != "":
    folder = results.pmdDataFolder

    try:
        os.mkdir(folder)
        print "created directory: %s" % folder
        os.mkdir(folder)
    except:
        pass

    sleeptime = results.sleeptime
    while (1):

        timestamp = time.time()

        helpers.clean_slave(r, folder)
        helpers.upload_folder(r, folder)
        helpers.download_folder(r, folder)

        oldFiles = os.listdir(folder)

        time.sleep(sleeptime)
