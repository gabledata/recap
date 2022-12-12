from recap.storage import fs, recap
from urllib.parse import urlparse


registry = {
    'http': recap,
    'file': fs,
    's3': fs,
}


def open(**config):
    url = urlparse(config['url'])
    return registry[url.scheme].open(**config)
