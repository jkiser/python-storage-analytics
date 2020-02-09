import os.path
import zipfile
import tempfile
import logging
import glob

import dateutil
import pandas as pd

"""
While Pandas does provide an I/O mechanism for compressed files, it will
only deal at present with ones containing a single file. It still
seems preferable to produce csv files limited to a configurable
number of lines, so this class is to provide a way of feeding
csv files on-the-fly to pandas.concat().
"""

logger = logging.getLogger(__name__)

class DataArchiveHandler(object):

    def __init__(self, path_to_zipfile):
        self.path = path_to_zipfile
        self._tempdir = None
        self._get_zipfile()

    @property
    def archive_contents(self):
        return self._archive_contents

    def _get_zipfile(self):
        logger.debug('%s._get_zipfile()' % self.__class__.__name__)
        try:
            self._zipfile = zipfile.ZipFile(self.path, 'r')
            self._archive_contents = [m for m in self._zipfile.namelist() if m.endswith('.csv')]
        except zipfile.BadZipfile as e:
            logger.error('Caught %s (file: %s)' % (e.__class__.__name__, self.path))

    def _cleanup(self):
        for f in glob.glob('%s*.csv' % self._tempdir):
            os.remove(f)
        os.rmdir(self._tempdir)

    def main(self):
        try:
            # create temp dir to extract csv files into
            self._tempdir = tempfile.mkdtemp()
            # build a collection of dataframe objects that will be turned
            # into one big one
            dataframes = []
            for c in self.archive_contents:
                extract = self._zipfile.extract(c, self._tempdir)
                dataframes.append(pd.read_csv(extract, converters={'mtime': dateutil.parser.parse}))
                os.remove(extract)
            os.rmdir(self._tempdir)
            # return a single dataframe
            return pd.concat(dataframes, ignore_index=True)
        except (KeyboardInterrupt, IOError):
            logger.error('Caught exception, cleaning up.')
            self._cleanup()
