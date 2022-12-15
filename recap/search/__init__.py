from recap.search import jq, recap


registry = {
    'jq': jq,
    'recap': recap,
}


# TODO Two different open methods feels hacky.
# TODO should type the return
def open_search(**config):
    backend = config['type']
    return registry[backend].open_search(**config)

def open_indexer(**config):
    backend = config['type']
    return registry[backend].open_indexer(**config)
