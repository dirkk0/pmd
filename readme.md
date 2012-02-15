# Poor Man's Dropbox

Synchronises files (no folders yet) via a Redis DB onto clients.

No production quality whatsoever - use at own risk!

## How to install
tbd

### Linux

sudo apt-get install python-pip
sudo pip install redis

## How to use

```
python pmd.py -c config/localhost.json -d data
```
### todo
- clean up/tests/docs
- user Redis pubsub instead of polling
