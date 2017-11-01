from collections import MutableMapping
from distributed.utils import log_errors


class PublishExtension(object):
    """ An extension for the scheduler to manage collections

    *  publish-list
    *  publish-put
    *  publish-get
    *  publish-delete
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.datasets = dict()

        handlers = {'publish_list': self.list,
                    'publish_put': self.put,
                    'publish_get': self.get,
                    'publish_delete': self.delete}

        self.scheduler.handlers.update(handlers)
        self.scheduler.extensions['publish'] = self

    def put(self, stream=None, keys=None, data=None, name=None, client=None):
        with log_errors():
            if name in self.datasets:
                raise KeyError("Dataset %s already exists" % name)
            self.scheduler.client_wants_keys(keys, 'published-%s' % name)
            self.datasets[name] = {'data': data, 'keys': keys}
            return {'status':  'OK', 'name': name}

    def delete(self, stream=None, name=None):
        with log_errors():
            out = self.datasets.pop(name, {'keys': []})
            self.scheduler.client_releases_keys(out['keys'], 'published-%s' % name)

    def list(self, *args):
        with log_errors():
            return list(sorted(self.datasets.keys()))

    def get(self, stream, name=None, client=None):
        with log_errors():
            if name in self.datasets:
                return self.datasets[name]
            else:
                raise KeyError("Dataset '%s' not found" % name)


class Datasets(MutableMapping):
    """A dict-like wrapper around :class:`Client` dataset methods.

    Parameters
    ----------
    client : distributed.client.Client

    """
    def __init__(self, client):
        self.__client = client

    def __getitem__(self, key):
        return self.__client.get_dataset(key)

    def __setitem__(self, key, value):
        self.__client.publish_dataset(**{key: value})

    def __delitem__(self, key):
        self.__client.unpublish_dataset(key)

    def __contains__(self, key):
        return key in self.__client.list_datasets()

    def __iter__(self):
        for key in self.__client.list_datasets():
            yield key

    def __len__(self):
        return len(self.__client.list_datasets())
