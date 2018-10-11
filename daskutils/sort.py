import daskutils.math
import daskutils.base
import daskutils.io.msgpack
import dask.bag
import dask.distributed
import os.path
import uuid
import msgpack
import itertools

def split(data):
    data = data.to_delayed()
    a = data[:len(data) // 2]
    b = data[len(data) // 2:]
    a = dask.bag.from_delayed(a)
    b = dask.bag.from_delayed(b)
    return a, b

def fileiter(filenames):
    for filename in filenames:
        with open(filename, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg

def merge(a, b):
    a = iter(a)
    b = iter(b)
    bpeek = None
    for aval in a:
        if bpeek is not None:
            if bpeek < aval:
                yield bpeek
                bpeek = None
        if bpeek is None:
            for bval in b:
                if bval < aval:
                    yield bval
                else:
                    bpeek = bval
                    break
        yield aval
    if bpeek is not None:
        yield bpeek
        bpeek = None
    for bval in b:
        yield bval

def writer(stream, filepattern, items_per_file):
    itemcounter = 0
    filecounter = 0
    outfile = None
    def newfile():
        global itemcounter
        global filecounter
        global outfile
    for val in stream:
        if itemcounter == 0 or itemcounter > items_per_file:
            itemcounter = 0
            filename = filepattern % filecounter
            if filecounter > 0:
                outfile.close()
            outfile = open(filename, "wb")
            filecounter += 1
            yield filename
        msgpack.dump(val, outfile)
        itemcounter += 1
    outfile.close()

class MergeSort(object):
    def __init__(self, tempdir, key=lambda a: a, partition_size=2000):
        self.tempdir = tempdir
        self.key = key
        self.partition_size = partition_size
        
    def sort(self, data):
        tempdir = self.tempdir
        mergeid = str(uuid.uuid4())
        return daskutils.io.msgpack.read(
            self.merge_sort(
                daskutils.io.msgpack.write(
                    self.sort_partitions(data),
                    os.path.join(tempdir, mergeid + "-%s.msgpack"))))

    def sort_partitions(self, bag):
        key = self.key
        @bag.map_partitions
        def res(lines):
            return sorted(lines, key=key)
        return res

    def merge(self, a, b):
        tempdir = self.tempdir
        key = self.key
        mergeid = str(uuid.uuid4())
        
        @dask.delayed
        def read_and_merge_bag():
            a_filenames = a.compute()
            b_filenames = b.compute()

            return dask.bag.from_sequence(
                writer(
                    merge(
                        fileiter(a_filenames),
                        fileiter(b_filenames)),
                    os.path.join(tempdir, mergeid + "-%s.msgpack"),
                    self.partition_size),
                partition_size = 1)

        return read_and_merge_bag().compute()
    
    def merge_sort(self, tosort, indent='>'):
        print(indent, "merge_sort", tosort.npartitions)
        if tosort.npartitions <= 1:
            print(indent, "single", tosort.npartitions)
            return tosort
        else:
            a, b = split(tosort)
            print(indent, "split", a.npartitions, b.npartitions)
            a = self.merge_sort(a, indent+"a")
            b = self.merge_sort(b, indent+"b")
            print(indent, "merge", a.npartitions, b.npartitions)
            return self.merge(a, b)
