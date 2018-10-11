import daskutils.math
import daskutils.base
import daskutils.io.msgpack
import dask.bag
import dask.distributed
import os.path
import uuid
import msgpack
import itertools

class MergeSort(object):
    def __init__(self, tempdir, key=lambda a: a):
        self.tempdir = tempdir
        self.key = key
        
    def sort(self, data):
        return self.merge_sort(self.sort_partitions(data))

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
            a_filenames = daskutils.io.msgpack.write(a, os.path.join(tempdir, mergeid + "-a-%s.json")).compute()
            b_filenames = daskutils.io.msgpack.write(b, os.path.join(tempdir, mergeid + "-b-%s.json")).compute()

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
                        else:
                            break
                    else:
                        for bval in b:
                            if bval < aval:
                                yield bval
                            else:
                                bpeek = bval
                                break
                    yield aval
                for bval in b:
                    yield bval

            return dask.bag.from_sequence(
                merge(fileiter(a_filenames), fileiter(b_filenames)),
                npartitions = len(a_filenames) + len(b_filenames))

        return read_and_merge_bag().compute()

    def split(self, data):
        data = data.to_delayed()
        a = data[:len(data) // 2]
        b = data[len(data) // 2:]
        a = dask.bag.from_delayed(a)
        b = dask.bag.from_delayed(b)
        return a, b
    
    def merge_sort(self, tosort, indent='>'):
        print(indent, "merge_sort", tosort.npartitions)
        if tosort.npartitions <= 1:
            print(indent, "single", tosort.npartitions)
            return tosort
        else:
            a, b = self.split(tosort)
            print(indent, "split", a.npartitions, b.npartitions)
            a = self.merge_sort(a, indent+"a")
            b = self.merge_sort(b, indent+"b")
            print(indent, "merge", a.npartitions, b.npartitions)
            return self.merge(a, b)
