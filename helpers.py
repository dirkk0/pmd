import json
import redis
import hashlib
import os
import sys
import cmd


def createHash(pwd):
    # print base64.b64encode("password") # print base64.b64decode("cGFzc3dvcmQ=")

    m = hashlib.sha1()
    pwd = m.update(pwd)
    pwd = m.hexdigest()
    # import base64
    # pwd = base64.b64encode(pwd)
    return pwd


def read_in_chunks(infile, chunk_size=1024 * 512):
    chunk = infile.read(chunk_size)
    while chunk:
        yield chunk
        chunk = infile.read(chunk_size)


def upload_all(r, folder):
    files = os.listdir(folder)

    for filename in files:
        upload(r, folder, filename)


def upload(r, folder, filename):
    "a file"
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
            print 'uploading', key

            infile = open(fullFileName, 'rb')
            for chunk in read_in_chunks(infile):
                r.rpush('file:%s:lbin' % key, chunk)
                print "chunk"
            r.set('file:%s:name' % key, filename)
            r.set('file:%s:hash' % filename, key)
            return key, True
        else:
            print "file was deleted before: %s" % filename


def download_all_user(r, username):
    foldername = 'cache_%s' % username
    download_all(foldername)


def download_all(r, foldername):

    keys = r.keys('file:*:hash')
    if keys:
        for key in keys:
            filename = key.split(':')[1]
            hashkey = r.get('file:%s:hash' % filename)
            # print 'Trying to download ' + filename
            if hashkey != "0":   # file was deleted!
                # print "test %s %s" % (hashkey, filename)
                download(r, hashkey, foldername, filename)


def abgleich_slave(r, folder):

    fFiles = os.listdir(folder)
    for filename in fFiles:
        if r.get('file:%s:hash' % filename) == "0":
            print 'I will key-delete ' + filename
            os.remove(os.path.join(folder, filename))


def download(r, key, foldername='', filename=''):

    if filename == '':
        filename = key

    fullFileName = os.path.join(foldername, filename)

    hash = r.get('file:%s:hash' % filename)
    # print filename, hash

    if os.path.exists(fullFileName):
        infile = open(fullFileName, 'rb')
        fileHash = createHash(infile.read())

        if (fileHash == hash):
            # nothing to do
            return

    # print fullFileName
    print 'downloading file. %s' % filename
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


def connect_db():
    # print "Loading configuration."
    configFile = os.path.join('config', 'localhost.json')
    # configFile = os.path.join('config', 'localhost.json')
    print 'Trying to load ' + configFile
    if not os.path.exists(configFile):
        print 'config file %s not found.' % configFile
        return
    configs = json.load(open(configFile, 'r'))

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
