import json
import redis
import hashlib
import os


def log(msg):
    print(msg)


def createHash(pwd):
    m = hashlib.sha1()
    pwd = m.update(pwd)
    pwd = m.hexdigest()
    return pwd


def read_in_chunks(infile, chunk_size=1024 * 512):
    chunk = infile.read(chunk_size)
    while chunk:
        yield chunk
        chunk = infile.read(chunk_size)


def upload_folder(r, foldername):
    files = os.listdir(foldername)
    for filename in files:
        if filename[0] != '.':
            upload_file(r, foldername, filename)


def upload_file(r, foldername, filename):
    fullFileName = os.path.join(foldername, filename)
    if not os.path.exists(fullFileName):
        print "File %s doesn't exist." % fullFileName
        return None, None

    dotFileName = os.path.join(foldername, '.' + filename)

    infile = open(fullFileName, 'rb')
    localFileHash = createHash(infile.read())

    redisFileVersion = r.get('file:%s:version' % filename)
    if redisFileVersion:
        redisFileVersion = int(redisFileVersion)
        redisFileHash = r.get('file:%s:hash' % filename)
        if os.path.exists(dotFileName):
            localFileVersion = open(dotFileName, 'rb').read()
            localFileVersion = int(localFileVersion)
            if (localFileVersion == redisFileVersion):
                if redisFileHash == localFileHash:
                    # versions are equal, file hashes are equal
                    # print("nothng to do")
                    return False, False

        # if not r.keys('binary:%s:filename' % localFileHash):
        #     log('binary already there 1. %s' % filename)
        #     r.sadd('binary:%s:filename' % localFileHash, filename)
        #     r.set('file:%s:hash' % filename, localFileHash)
        #     redisVersion = r.incr('file:%s:version' % filename)
        #     open(dotFileName, 'wb').write(str(redisVersion))
        #     return localFileHash, False
        # else:
        # if redisFileHash != localFileHash:
                else:
                    if not r.exists('binary:%s:lbin' % localFileHash):
                        log("ok, we need to upload 1")
                        infile = open(fullFileName, 'rb')
                        for chunk in read_in_chunks(infile):
                            r.rpush('binary:%s:lbin' % localFileHash, chunk)
                            # print "chunk"
                    r.sadd('binary:%s:filename' % localFileHash, filename)
                    r.set('file:%s:hash' % filename, localFileHash)

                    redisVersion = r.incr('file:%s:version' % filename)
                    print redisFileVersion
                    print "...wtfready."

                    open(dotFileName, 'wb').write(str(redisVersion))
                    return localFileHash, True

    else:
        # totally new, name is not in redis yet.
        print 'New file \'%s\':' % (filename)
        print 'Upload %s ...' % (filename)

        # but binary might be there.
        # if redisFileHash != localFileHash:
        if not r.exists('binary:%s:lbin' % localFileHash):
            log("ok, we need to upload 1")
            infile = open(fullFileName, 'rb')
            for chunk in read_in_chunks(infile):
                r.rpush('binary:%s:lbin' % localFileHash, chunk)
                # print "chunk"
        r.sadd('binary:%s:filename' % localFileHash, filename)
        r.set('file:%s:hash' % filename, localFileHash)

        redisVersion = r.incr('file:%s:version' % filename)
        print "...uready."

        open(dotFileName, 'wb').write(str(redisVersion))
        return localFileHash, True


def download_folder(r, foldername):
    keys = r.keys('file:*:hash')
    if keys:
        for key in keys:
            filename = key.split(':')[1]
            hashkey = r.get('file:%s:hash' % filename)
            # print 'Trying to download ' + filename
            if hashkey != "0":   # file was deleted!
                # print "test %s %s" % (hashkey, filename)
                download_file(r, hashkey, foldername, filename)


def clean_slave(r, folder):

    files = []
    dotFiles = []
    fFiles = os.listdir(folder)
    for filename in fFiles:
        if filename[0] == '.':
            dotFiles.append(filename[1:])
        else:
            files.append(filename)
    # print files, dotFiles
    diff = set(dotFiles).difference(set(files))
    if diff:
        print diff


            # if r.get('file:%s:hash' % filename) == "0":
            #     print 'Key-delete: ' + filename
            #     os.remove(os.path.join(folder, filename))


def download_file(r, hashkey, foldername='', filename=''):

    if filename == '':
        filename = hashkey

    fullFileName = os.path.join(foldername, filename)
    dotFileName = os.path.join(foldername, '.' + filename)

    redisFileVersion = int(r.get('file:%s:version' % filename))
    # print filename, hash

    if os.path.exists(dotFileName):
        localFileVersion = open(dotFileName, 'rb').read()
        localFileVersion = int(localFileVersion)

        if (localFileVersion == redisFileVersion):
            # nothing to do
            return

    print 'New file \'%s\' (v%s, h%s):' % (filename, redisFileVersion, hashkey)
    print 'Download ...'
    num = r.llen('binary:%s:lbin' % hashkey)
    l = []
    # print num
    for i in range(num):
        l.append(r.lrange('binary:%s:lbin' % hashkey, i, i)[0])
        # print '.'
    #t = r.smembers(key)
    bin = ''.join(l)

    open(fullFileName, 'wb').write(bin)
    open(dotFileName, 'wb').write(str(redisFileVersion))
    print '... dready.'


def connect_db(configFileName):
    # print "Loading configuration."
    # configFile = os.path.join('config', 'server4you.json')

    print 'Trying to load ' + configFileName
    if not os.path.exists(configFileName):
        print 'config file %s not found.' % configFileName
        return
    configs = json.load(open(configFileName, 'r'))

    r = redis.Redis(host=configs['redisHost'], port=configs['redisPort'], password=configs['redisPassword'], db=0)
    # r2 = redis.Redis(host=configs['redisHost'], port=configs['redisPort'], password=configs['redisPassword'], db=0)

    # p = r.pubsub()

    try:
        r.ping()
        # print "Found Datbase."
        print "Connected to %s." % configs['redisHost']
        return r
    except redis.ConnectionError:
        # print redis.ConnectionError
        print "No Redis, sry. (tried %s)" % configs['redisHost']
