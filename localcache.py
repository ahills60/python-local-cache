import os, gzip, pickle, hashlib, shutil, itertools
class localcache:
    """
    Local cache class for managing a local cache of files

    Author: Andrew Hills (a.hills@sheffield.ac.uk)
    Data: 2019-02-07
    """

    def __init__(self):
        """
        Local Cache initialisation.
        """

        self._sourceList = []
    
    def add_source(self, datasource = None):
        """
        Add a data source for locating files
        """
        
        if datasource is None:
            raise TypeError("Expected data source to be specified.")

        if isinstance(datasource, list):
            self._sourceList.extend([item for item in datasource if os.path.isdir(item)])
        elif isinstance(datasource, str) and os.path.isdir(datasource):
            self._sourceList.append(datasource)
        else:
            raise TypeError("Unable to determine data source type.")

    def scan_files(self, datasource = None):
        """
        Scan the specified data source for files
        """
        fileList = os.listdir(datasource)

        # Now remove directories:
        fileList = [item for item in fileList if not os.path.isdir(item)]
        # Generate filename
        outFile = "{}.sav".format(hashlib.sha1(os.path.abspath(datasource).encode()).hexdigest())

        self._save_and_compress(os.path.join(self._get_local_repo_base_path(), "meta", "dir", outFile), data = {"fileList": fileList})

    def _get_local_repo_base_path(self):
        """
        Return the base path of the local file cache
        """
        return os.path.join(os.path.expanduser('~'), ".localcache")

    def _save_and_compress(self, filename = None, data = None):
        """
        Save and compress give data to the provided filename.
        """
        if os.path.exists(filename):
            os.remove(filename)
        
        fileContents = gzip.open(filename, 'wb', compresslevel = 3)
        pickle.dump(data, fileContents, protocol = pickle.HIGHEST_PROTOCOL)
        fileContents.close()
    
    def _load_compressed_file(self, filename):
        """
        Load a filename, decompress and return the data.
        """
        if os.path.exists(filename):
            fileContents = gzip.open(filename, 'rb')
            output = pickle.load(fileContents)
            fileContents.close()
            return output
        else:
            raise FileNotFoundError("Unable to file \"{}\"".format(filename))
    
    def _get_local_repo_size(self):
        """
        Returns the size (in bytes) of the local repository
        """
        return sum(item.stat().st_size for item in os.scandir(os.path.join(self._get_local_repo_base_path, "cache")))


    def init_setup(self):
        """
        Create initialise base structure for the local cache
        """
        pathList = [["meta", "dir"], ["meta", "files"], ["cache"]]
        
        for child in pathList:
            os.makedirs(os.path.join(self._get_local_repo_base_path(), *child))

        hexvals = [hex(val)[-1] for val in range(16)]
        combs = ["{}{}".format(*item) for item in itertools.product(hexvals, hexvals)]

        for item in combs:
            os.makedirs(os.path.join(self._get_local_repo_base_path(), "cache", item))
