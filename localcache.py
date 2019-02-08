import os
import gzip
import pickle
import hashlib
import shutil
import itertools

class localcache:
    """
    Local cache class for managing a local cache of files

    Author: Andrew Hills (a.hills@sheffield.ac.uk)
    Data: 2019-02-07
    """

    def __init__(self, verbose = False):
        """
        Local Cache initialisation.
        """

        self._sourceList = []
    
    def _verb(self, message = ""):
        """
        Print a message if verbose
        """
        print("{}: {}".format(self.__class__, message))

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

    def add_file_to_cache(self, filename, datastore = None):
        """
        Add the given file to the local file cache
        """
        if datastore is not None:
            fullpath = os.path.join(datastore, filename)
        else:
            fullpath = filename
            filename = os.path.basename(filename)

        hashval = hashlib.sha1(filename.encode()).hexdigest()
        shutil.copy(fullpath, os.path.join(self._get_local_repo_base_path, "cache", hashval[:2], filename))

    def save_states(self):
        """
        Save the current state of the local cache
        """
        self._save_and_compress(os.path.join(self._get_local_repo_base_path(), "sources.state"), {"datasource": self._sourceList})
    
    def load_states(self):
        """
        Restore the state of the local cache
        """
        try:
            states = self._load_compressed_file(os.path.join(self._get_local_repo_base_path(), "sources.state"))
            self._sourceList = states['datasource']
        except:
            pass
    
    def _hash_file_contents(self, filename = None):
        """
        Hash the contents of a file
        """

        sha1 = hashlib.sha1()
        with open(filename, 'rb') as file:
            while True:
                # Read in chunks
                data = file.read(65536)
                if not data:
                    file.close()
                    break
                sha1.update(data)
        return sha1.hexdigest()