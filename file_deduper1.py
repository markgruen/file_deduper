#!/usr/bin/env /usr/bin/python2.7
"""
Usage:
  file_deduper [--skip=SKIP | --skip-path=SKIPPATH] PATH...

Arguments:
  PATH           Space separated list of path to check for duplicates

Options:
  -h --help             show this help message and exit
  --skip SKIP           Comma separated list of paths to skip
  --skip-path SKIPPATH  File containing 1 path per line to skip

"""
import sys
import os
from datetime import datetime
from tqdm import tqdm
from docopt import docopt
from schema import Schema, And, Optional, Or, Use, SchemaError
import hashlib

__version__ = 0.1


def escape_filename(filename):
    return filename.replace(' ', '\ ')\
        .replace('[', '\[')\
        .replace(']', '\]')\
        .replace('(', '\(')\
        .replace(')', '\)')\
        .replace('&', '\&')\
        .replace('?', '\?')\
        .replace("'", "\'")
                   

def chunk_reader(fobj, chunk_size=1024*256):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


# TODO may want to increase first chuck to 50 to reduce 7359 matches on 10
def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
    hashobj = hash()
    with open(filename, 'rb') as file_object:
        if first_chunk_only:
            hashobj.update(file_object.read(1024*1024))
        else:
            for chunk in chunk_reader(file_object):
                hashobj.update(chunk)
        hashed = hashobj.digest()
    return hashed


def skip_dir(thepath, skip_paths):
    skip = False
    thepath_parts = thepath.split(os.sep)
    for sp in skip_paths:
        l = len(sp.split(os.sep))
        if sp == os.sep.join(thepath_parts[0:l]):
            skip = True
            break
    return skip


def get_skips(args):
    if args['--skip']:
        skip_paths = args['--skip']
    elif args['--skip-path']:
        skip_paths = args['--skip-path'].read().splitlines()
        args['--skip-path'].close()
    else:
        skip_paths = ''
    return skip_paths


def check_for_duplicates(args, hash=hashlib.sha1):
    hashes_by_size = {}
    hashes_on_1k = {}
    hashes_full = {}
    skip_paths = None
    timing = []
    timing.append({})
    timing.append({})
    timing.append({})
    paths = args['PATH']
    skip_paths = get_skips(args)

    print('#Starting first scan by file size')
    print('#Checking root path: {} for duplicates'.format(paths[0]))
    timing[0]['start'] = datetime.now()
    for path in paths:
        print('#  checking path: {}'.format(path))
        for dirpath, dirnames, filenames in os.walk(path):
            if skip_dir(dirpath, skip_paths):
                print("#     skipping {}".format(dirpath))
                continue
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    pass

                duplicate = hashes_by_size.get(file_size)

                if duplicate:
                    hashes_by_size[file_size].append(full_path)
                else:
                    hashes_by_size[file_size] = []  # create the list for this file size
                    hashes_by_size[file_size].append(full_path)
    timing[0]['end'] = datetime.now()
    print('#Completed first scan by file size: {} count: {}'.format(timing[0]['end']-timing[0]['start'], len(hashes_by_size)))

    # For all files with the same file size, get their hash on the 1st 1024 bytes
    print('#Starting second scan by small chunk size')
    timing[1]['start'] = datetime.now()
    for __, files in tqdm(hashes_by_size.items()):
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
            except OSError, e:
                print(e)
                continue

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)
    timing[1]['end'] = datetime.now()
    print('#Completed second scan by small chunk size: {} count: {}'.format(timing[1]['end']-timing[1]['start'], len(hashes_on_1k)))

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    print('#Starting third scan by complete file')
    timing[2]['start'] = datetime.now()
    for  __, files in tqdm(hashes_on_1k.items()):
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpy cycles on it

        skip_files = ['txt','nfo']
        for filename in files:
            try:
                if filename.split('.')[-1].lower() in skip_files and duplicate.split('.')[-1].lower() in skip_files:
                    continue
            except AttributeError, e:
                continue

            try:
                full_hash = get_hash(filename, first_chunk_only=False)
            except OSError, e:
                print(e)
                continue

            duplicate = hashes_full.get(full_hash)
            if duplicate:
                hashes_full[full_hash].append(filename)
            else:
                hashes_full[full_hash] = []          # create the list for dup files
                hashes_full[full_hash].append(filename)
    timing[2]['end'] = datetime.now()
    print('#Completed third scan by complete file: {} count {}'.format(timing[2]['end']-timing[2]['start'], len(hashes_full)))

    print('# List of duplicates')
    for __, files in hashes_full.items():
        if len(files) < 2:
            continue
        print('\n'.join([escape_filename(f) for f in files]))
        print('')
    print('')
    print('Timing summary')
    print('  Scan building dict by file size: {} count {}'.format(timing[0]['end']-timing[0]['start'], len(hashes_by_size)))
    print('  Scan building dict by 1k hash: {} count {}'.format(timing[1]['end']-timing[1]['start'], len(hashes_on_1k)))
    print('  Scan building dict by all hash: {} count {}'.format(timing[2]['end']-timing[2]['start'], len(hashes_full)))


if __name__ == '__main__':
    args = docopt(__doc__, version=__version__)
    print(args)
    print('')

    schema = Schema({
            'PATH': And([os.path.isdir], error='Must be valid paths'),
            '--skip-path': Use(open),
            object: object
            })
    try:
        args = schema.validate(args)
    except SchemaError as e:
        sys.exit(e)

    check_for_duplicates(args)
