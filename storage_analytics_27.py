import os
import os.path
import sys
import csv
import zipfile
import tempfile
import shutil
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

"""
This module provides a utility class that will gather various file system
storage data (starting point paths are provided by user) and output a
zip archive containing csv files. Currently, as a standalone script, only
starting point paths may be provided as arguments, but if the StorageReporter
class is imported, a number of other attributes may be configured:

paths: one or more paths on some file system
to_csv: output data as csv (default is True, and currently is the only
supported format
csv_fieldnames: although currently only mtime and bytes are retrieved
from the call to stat made on each file, this option does allow for
modifying the fieldnames in the csv header. Eventually this should
become a more flexible option that would allow for any set of valid
data provided by stat.
output_path: path where generated zip file should be written (default is
directory where script is run)
output_file_name: name for generated zip file
(default: storage_analytics_data.zip)
max_csv_lines: each csv file will be limited to this number. Once limit
is reached, a new csv, whose filename will be incremented by 1, will
be created.
"""


class StorageReporter(object):
    def __init__(self, paths, to_csv=True, csv_fieldnames=('mtime', 'bytes'), output_path=None,
                 output_file_name='storage_analytics_data.zip', max_csv_lines=100000):
        if isinstance(paths, basestring):
            self.paths = [paths]
        else:
            self.paths = paths
        self.to_csv = to_csv
        self.max_csv_lines = max_csv_lines
        self.csv_fieldnames = csv_fieldnames
        self.run_dir = os.getcwd()
        self.output_path = output_path or self.run_dir
        self.output_file_name = output_file_name
        self.csv_file_sequence_number = 0  # will be used for csv name
        self._tempdir = None
        self._current_csv = None

    @property
    def tempdir(self):
        if not self._tempdir:
            self._tempdir = tempfile.mkdtemp()
        return self._tempdir

    @property
    def csv_filename(self):
        file_name = '%s__%04d.csv' % (os.path.splitext(self.output_file_name)[0], self.csv_file_sequence_number)
        self.csv_file_sequence_number += 1
        return file_name

    def generate_paths(self):
        for path in self.paths:
            for root, dirs, files in os.walk(path, onerror=self.log_error):
                for f in files:
                    yield os.path.join(root, f)

    def log_error(self, error):
        sys.stderr.write('Error: %s\n' % error.filename)

    def make_record(self, path):
        return (self.handle_mtime(os.path.getmtime(path)), os.path.getsize(path))

    def clean_up(self):
        if self._tempdir:
            print('Cleaning up temporary data ...')
            shutil.rmtree(self._tempdir)

    def main(self):
        os.chdir(self.tempdir)
        logger.debug('main() => os.chdir(%s)' % os.getcwd())
        # iterate through root paths
        file_data = []  # queue
        try:
            for p in self.generate_paths():
                # here we monitor our max number of lines to pass to csv.
                if len(file_data) == self.max_csv_lines:
                    logger.debug('main() => len(file_data) == %d' % self.max_csv_lines)
                    self.make_csv(file_data)
                    self.add_to_zip()
                    # clear queue
                    logger.debug('main() => file_data = []')
                    file_data = []
                else:
                    try:
                        file_data.append(self.make_record(p))
                    except OSError:
                        logger.warn('Skipping path %s' % p)
            logger.debug('main() => writing %d to csv' % len(file_data))
            self.make_csv(file_data)
            self.add_to_zip()
            # jump back to starting point, then move our zip
            os.chdir(self.run_dir)
            logger.debug('main() => shutil.move()')
            shutil.move(
                os.path.join(self.tempdir, self.output_file_name),
                os.path.join(self.run_dir, self.output_file_name)
            )
            # remove temp dir
            logger.debug('main() => or.rmdir()')
            os.rmdir(self.tempdir)
        except (KeyboardInterrupt, IOError):
            # if the script is killed or runs into some space problem,
            # remove whatever temporary stuff we've created.
            self.clean_up()

    def add_to_zip(self):
        logger.debug('add_to_zip()')
        # we'll keep zip as a temp file till we're done
        mode = 'a'
        if not os.path.exists(self.output_file_name):
            mode = 'w'
        with zipfile.ZipFile(self.output_file_name, mode) as z:
            z.write(self._current_csv)
            # purge temp csv file
            os.remove(self._current_csv)

    def make_csv(self, lines):
        logger.debug('make_csv()')
        # generate next csv filename
        self._current_csv = self.csv_filename
        with open(self._current_csv, 'wb') as c:
            w = csv.DictWriter(c, fieldnames=self.csv_fieldnames)
            try:
                w.writeheader()
            except AttributeError:
                # still a python 2.6.x out there, so writeheader() not supported
                w.writerow(dict(zip(self.csv_fieldnames, self.csv_fieldnames)))
            for l in lines:
                w.writerow(dict(zip(self.csv_fieldnames, l)))
        sys.stdout.write('%s lines written to %s\n' % (len(lines), self._current_csv))

    def handle_mtime(self, mtime):
        return datetime.fromtimestamp(mtime)

if __name__ == '__main__':
    reporter = StorageReporter(sys.argv[1:])
    reporter.main()
