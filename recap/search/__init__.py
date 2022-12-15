from recap.search import jq, recap
from recap.storage.abstract import AbstractStorage
from urllib.parse import urlparse


registry = {
    'jq': jq,
    # TODO Rethink this, since Elastic HTTP end points would conflict.
    'http': recap,
}


# TODO should type the return
def open(storage: AbstractStorage, **config):
    url = urlparse(config['url'])
    return registry[url.scheme].open(storage, **config)
