from __future__ import absolute_import, division, print_function

from fnmatch import fnmatch


def filenames(hdfs, path):
    """
    Filenames in Hadoop File System under specific path

    Parameters
    ----------

    hdfs: pywebhdfs.webhdfs.PyWebHdfsClient instance
    path: string
        Directory on HDFS

    Path can be either

    1.  A directory -- 'user/data/myfiles/'
    2.  A globstring -- 'user/data/myfiles/*/*.json'

    Returns all filenames within this directory and all subdirectories
    """
    if '*' in path:
        directory = path[:path.find('*')].rsplit('/', 1)[0]
        return [fn for fn in filenames(hdfs, directory)
                if fnmatch(fn, path + '*')]

    path = path.strip('/')
    listdir = hdfs.list_dir(path)['FileStatuses']['FileStatus']

    files = ['%s/%s' % (path, d['pathSuffix'])
             for d in listdir if d['type'] == 'FILE']
    directories = ['%s/%s' % (path, d['pathSuffix'])
                   for d in listdir if d['type'] == 'DIRECTORY']
    return files + sum([filenames(hdfs, d) for d in directories], [])
