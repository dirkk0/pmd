import sys
import helpers
import time

import os


r = helpers.connect_db()

if not r:
    sys.exit(1)


# helpers.download_all(r, 'cache')
if len(sys.argv) > 1:
    folder = sys.argv[1]

    try:
        os.mkdir(folder)
        print "created directory: %s" % folder
    except:
        pass

    oldFiles = os.listdir(folder)

    sleeptime = 1
    while (1):

        timestamp = time.time()

        currentFiles = os.listdir(folder)
        oldSet = set(oldFiles)
        currentSet = set(currentFiles)

        # print sfFiles, svFiles

        # was a file deleted?
        delList = list(oldSet.difference(currentSet))
        if delList:
            print "del:"
            print delList
            for filename in delList:
                r.set('file:%s:hash' % filename, "0")

        # was a file added?
        addList = list(currentSet.difference(oldSet))
        if addList:
            print "add: "
            print addList
            for filename in addList:
                r.delete('file:%s:hash' % filename)

        # was a file changed?
        for filename in currentFiles:
            fullFilename = os.path.join(folder, filename)
            # print filename, time.time() - timestamp
            if (timestamp - os.stat(fullFilename).st_mtime) < sleeptime * 1.5:
                print "%s changed!" % filename
                # r.delete('file:%s:hash' % filename)
                r.set('file:%s:hash' % filename, "-1")
                # helpers.upload(r, folder, filename)

        helpers.clean_slave(r, folder)
        helpers.upload_folder(r, folder)
        helpers.download_folder(r, folder)

        oldFiles = os.listdir(folder)

        time.sleep(sleeptime)
