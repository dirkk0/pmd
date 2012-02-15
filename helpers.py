import json
import redis
import hashlib
import os


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


def upload_folder(r, folder):
    files = os.listdir(folder)
    for filename in files:
        upload_file(r, folder, filename)


def upload_file(r, folder, filename):
    fullFileName = os.path.join(folder, filename)
    if not os.path.exists(fullFileName):
        print "File %s doesn't exist." % fullFileName
        return None, None

    infile = open(fullFileName, 'rb')

    key = createHash(infile.read())

    if r.keys('file:%s:name' % key) and r.get('file:%s:hash' % filename) != "-1":
        # print 'binary already there. %s' % filename
        if not r.keys('file:%s:hash' % filename):
            print 'setting key.' + key
            r.set('file:%s:hash' % filename, key)
            return key, False
    else:
        if r.get('file:%s:hash' % filename) != "0":
            print 'uploading %s ...' % (filename)

            infile = open(fullFileName, 'rb')
            r.delete('file:%s:lbin' % key)
            for chunk in read_in_chunks(infile):
                r.rpush('file:%s:lbin' % key, chunk)
                # print "chunk"
            r.set('file:%s:name' % key, filename)
            r.set('file:%s:hash' % filename, key)
            print "...upload ready."
            return key, True
        else:
            print "file was deleted before: %s" % filename


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

    fFiles = os.listdir(folder)
    for filename in fFiles:
        if r.get('file:%s:hash' % filename) == "0":
            print 'Key-delete: ' + filename
            os.remove(os.path.join(folder, filename))


def download_file(r, key, foldername='', filename=''):

    if filename == '':
        filename = key

    fullFileName = os.path.join(foldername, filename)

    redisFileHash = r.get('file:%s:hash' % filename)
    # print filename, hash

    if os.path.exists(fullFileName):
        infile = open(fullFileName, 'rb')
        localFileHash = createHash(infile.read())

        if (localFileHash == redisFileHash):
            # nothing to do
            return

    # print fullFileName
    print 'Downloading file: %s (v)' % (filename)
    num = r.llen('file:%s:lbin' % key)
    l = []
    # print num
    for i in range(num):
        l.append(r.lrange('file:%s:lbin' % key, i, i)[0])
        # print '.'
    #t = r.smembers(key)
    bin = ''.join(l)

    open(fullFileName, 'wb').write(bin)
    # print 'ready'


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
