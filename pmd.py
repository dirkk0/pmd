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
    except:
        pass

    oldFiles = os.listdir(folder)
    changeSet = []

    sleeptime = 1
    while (1):

        timestamp = time.time()

        currentFiles = os.listdir(folder)
        oldSet = set(oldFiles)
        currentSet = set(currentFiles)

        # print sfFiles, svFiles

        # was a file deleted by the user?
        delList = list(oldSet.difference(currentSet))
        if delList:
            # print "del:"
            # print delList
            for filename in delList:
                hashkey = r.get('file:%s:hash' % filename)
                print hashkey
                r.set('file:%s:hash' % filename, "0")
                r.delete('file:%s:name' % hashkey)
                r.delete('file:%s:lbin' % hashkey)

        # was a file added by the user?
        addList = list(currentSet.difference(oldSet))
        if addList:
            # print "add: "
            # print addList
            for filename in addList:
                r.delete('file:%s:hash' % filename)

        # was a file changed by the user?
        for filename in currentFiles:
            if filename in changeSet:
                # file was changed by the system, not the user
                pass
            else:
                fullFilename = os.path.join(folder, filename)
                # print filename, time.time() - timestamp
                if (timestamp - os.stat(fullFilename).st_mtime) < sleeptime * 1.5:
                    print "%s changed!" % filename
                    r.set('file:%s:hash' % filename, "-1")

        helpers.clean_slave(r, folder)
        helpers.upload_folder(r, folder)
        helpers.download_folder(r, folder)

        oldFiles = os.listdir(folder)

        # was a file changed by the system? do not upload again.
        changeSet = list(set(oldFiles).difference(set(currentFiles)))
        if changeSet:
            print "changeList:"
            print changeSet

        time.sleep(sleeptime)
