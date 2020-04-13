import os.path
import glob
import os
import tempfile
import zipfile
import csv

"""
a few OPENN related utilities.
"""

class ContentCsvPacker(object):

    def __init__(self, path='/openn/sitedata/Data', output_file='openn_contents.zip'):
        self.path = path
        self.output_file = output_file
        self._initial_dir = os.getcwd()
        os.chdir(self.path)
        self.tempdir = tempfile.mkdtemp()
        self.aggregate_csv = os.path.join(self.tempdir, 'temp.csv')

    def generate_filenames(self):
        filenames = [x for x in glob.glob('*.csv')]
        for item in enumerate(filenames):
            yield item

    def get_csv_header(self, records):
        return records[0].keys()

    def concat_csv(self):
        for num, name in self.generate_filenames():
            records = []
            with open(name) as csv_in:
                reader = csv.DictReader(csv_in)
                for line in reader:
                    records.append(line)
            with open(self.aggregate_csv, 'ab') as csv_out:
                writer = csv.DictWriter(csv_out, fieldnames=self.get_csv_header(records))
                if num == 0:
                    writer.writeheader()
                for record in records:
                    writer.writerow(record)






def gather_contents_csv(path='/openn/sitedata/Data', filename_pattern='????_contents.csv', output_file='openn_contents.zip'):
    """
    stuff the openn contents csv files into a zip for processing
    :param path:
    :param filename_pattern:
    :return:
    """
    start_dir = os.getcwd()
    os.chdir(path)
    with zipfile.ZipFile(os.path.join(start_dir, output_file), 'w') as z:
        count = 0
        for c in glob.glob(filename_pattern):
            z.write(c)
            count += 1
        print('%s files added to %s' % (count, output_file))
    os.chdir(start_dir)


def build_path_sets_from_csv(input_path, output_path=None):
    """
    idea here is to better map representation of create date to what can
    be obtained from direct querying of files.
    :param path:
    :return:
    """