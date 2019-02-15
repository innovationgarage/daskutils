import daskutils.math
import daskutils.base
import daskutils.io.msgpack
import dask.bag
import dask.distributed
import os.path
import uuid
import msgpack
import itertools
import contextlib
import socket
 
@contextlib.contextmanager
def debugopen(filename, *arg, **kw):
    try:
        with open(filename, *arg, **kw) as f:
            yield f
    except FileNotFoundError:
        base = existing_base(filename)
        if os.path.exists("/etc/host-hostname"):
            with open("/etc/host-hostname") as f:
                hostname = f.read().strip()
        else:
            hostname = socket.gethostname()
        raise FileNotFoundError("%s:%s: only %s exists, containing %s" % (hostname, filename, base, os.listdir(base)))


def merge(a, b, key):
    """Merges data from two iterators of sorted values into one
    iterator of sorted values."""
    
    a = iter(a)
    b = iter(b)
    bpeek = None
    for aval in a:
        if bpeek is not None:
            if key(bpeek) < key(aval):
                yield bpeek
                bpeek = None
        if bpeek is None:
            for bval in b:
                if key(bval) < key(aval):
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


class SortUnit(object):
    def __init__(self, mergesort, minval=None, maxval=None, count=None, data=None, a=None, b=None):
        assert count is not None
        self.mergesort = mergesort
        self.minval = minval
        self.maxval = maxval
        self.count = count
        self.data = data
        self.a = a
        self.b = b

    def construct(self, *arg, **kwarg):
        return SortUnit(self.mergesort, *arg, **kwarg)
        
    @dask.delayed
    def merge(self, other):
        # print("Merging %s[%s,%s] and %s[%s,%s]" % (self.count, self.minval, self.maxval, other.count, other.minval, other.maxval))
        if other is None:
            return self
        elif self.data and other.data:
            return self.merge_simple(other).compute()
        elif self.data:
            return other.merge(self).compute()
        else:
            return self.merge_splitted(other.split(self.b.minval), other).compute()

    @dask.delayed
    def merge_splitted(self, items, other):
        a, b = items
        @dask.delayed
        def construct(a, b):
            return self.construct(
                minval=min(self.minval, other.minval),
                maxval=max(self.maxval, other.maxval),
                count=self.count + other.count,
                a=a,
                b=b)
        return construct(self.a.merge(a), self.b.merge(b)).compute()
        
    @dask.delayed
    def split(self, value):
        if self.maxval < value:
            return self, None
        elif self.minval > value:
            return None, self
        elif self.data:
            return self.split_simple(value).compute()
        else:
            if value < self.b.minval:
                a, b = self.a.split(value).compute()
                if b is None:
                    b = self.b
                else:
                    b = self.construct(
                        minval=b.minval,
                        maxval=self.maxval,
                        count=b.count + self.b.count,
                        a=b,
                        b=self.b)
                return a, b
            else:
                a, b = self.b.split(value).compute()
                if a is None:
                    a = self.a
                else:
                    a = self.construct(
                        minval=self.minval,
                        maxval=a.maxval,
                        count=self.a.count + a.count,
                        a=self.a,
                        b=a)
                return a, b

    @dask.delayed
    def split_simple(self, value):
        assert self.data

        tempdir = self.mergesort.tempdir
        key = self.mergesort.key

        aout = self.construct(minval=self.minval, count=0, data=self.mergesort.tempfile(self.data, op="split"))
        bout = self.construct(minval=value, count=0, maxval=self.maxval, data=self.mergesort.tempfile(self.data, op="split"))
        
        with debugopen(self.data, 'rb') as f:
            with debugopen(aout.data, 'wb') as aof:
                with debugopen(bout.data, 'wb') as bof:
                    for line in msgpack.Unpacker(f, raw=False):
                        if key(line) < value:
                            msgpack.dump(line, aof)
                            aout.maxval=key(line)
                            aout.count += 1
                        else:
                            msgpack.dump(line, bof)
                            bout.count += 1
        
        return aout, bout
            
    @dask.delayed
    def merge_simple(self, other):
        assert self.data
        assert not self.a
        assert not self.b
        assert other.data
        assert not other.a
        assert not other.b
        
        tempdir = self.mergesort.tempdir
        key = self.mergesort.key

        aout = self.construct(count=0, data=self.mergesort.tempfile(self.data, other.data, op="merge"))
        bout = self.construct(count=0, data=self.mergesort.tempfile(self.data, other.data, op="merge"))
        
        with debugopen(self.data, 'rb') as af:
            with debugopen(other.data, 'rb') as bf:
                with debugopen(aout.data, 'wb') as aof:
                    with debugopen(bout.data, 'wb') as bof:
                        merged = iter(enumerate(merge(
                            msgpack.Unpacker(af, raw=False),
                            msgpack.Unpacker(bf, raw=False),
                            key)))
                        
                        for idx, line in merged:
                            if idx < self.mergesort.partition_size:
                                out = aout
                                of = aof
                            else:
                                out = bout
                                of = bof
                            if out.minval is None:
                                out.minval = key(line)
                            out.maxval = key(line)
                            out.count += 1
                            msgpack.dump(line, of)

        if bout.count == 0:
            return aout
                            
        return self.construct(
            minval=min(aout.minval, bout.minval),
            maxval=max(aout.maxval, bout.maxval),
            count=self.count + other.count,
            a=aout,
            b=bout)

    def flatten(self):
        if self.data:
            yield self
        else:
            for child in (self.a, self.b):
                for item in child.flatten():
                    yield item

    def read(self):
        assert self.data
        with debugopen(self.data, 'rb') as f:
            for line in msgpack.Unpacker(f, raw=False):
                yield line
        
class MergeSort(object):
    def __init__(self, tempdir, key=lambda a: a, partition_size=100000):
        self.tempdir = tempdir
        self.key = key
        self.partition_size = partition_size

    def tempfile(self, *parents, **kw):
        fileid = str(uuid.uuid4())[:4]
        if "op" in kw:
            fileid = fileid + "-" + kw["op"]
        if parents:
            parents = [os.path.split(parent)[1][:-len(".msgpack")].split("-")[0] for parent in parents]
            if len(parents) == 1:
                parents = parents[0]
            else:
                parents = "{%s}" % (",".join(parents))
            fileid = fileid + "-" + parents
        return os.path.join(self.tempdir, "%s.msgpack" % (fileid,))
        
    def sort(self, data):
        filenames = data.map_partitions(self.repartition_and_save)
        sort_units = [dask.delayed(sort_unit) for sort_unit in filenames.map(self.file_to_sort_unit).compute()]
        sort_unit = self.merge_sort(sort_units)
        sort_unit = sort_unit.compute()

        data = dask.bag.from_sequence(sort_unit.flatten(), 1)
        
        @data.map_partitions
        def data(part):
            return part[0].read()
                
        return data

    def repartition_and_save(self, data):
        partitionid = self.tempfile()
        files = []
        f = None
        p = 0
        for idx, item in enumerate(data):
            part = idx // self.partition_size
            if f is None or part != p:
                if f: f.close()
                p = part
                filename = self.tempfile(partitionid)
                f = open(filename, 'wb')
                files.append(filename)
            msgpack.dump(item, f)
        if f: f.close()
        return files
    
    def file_to_sort_unit(self, filename):
        outfilename = self.tempfile(filename, op="sort")

        with debugopen(filename, 'rb') as inf:
            data = sorted(msgpack.Unpacker(inf, raw=False), key=self.key)

        with debugopen(outfilename, 'wb') as outf:
            for item in data:
                msgpack.dump(item, outf)
                
        return self.sort_unit(minval=self.key(data[0]), maxval=self.key(data[-1]), count=len(data), data=outfilename)
    
    def sort_unit(self, *arg, **kwarg):
        return SortUnit(self, *arg, **kwarg)

    @dask.delayed
    def merge(self, a, b):
        return a.merge(b).compute()
    
    def merge_sort(self, sort_units, indent='>'):
        count = len(sort_units)
        if count <= 1:
            return sort_units[0]
        else:
            acount = count // 2
            a = self.merge_sort(sort_units[:acount], indent+"a")
            b = self.merge_sort(sort_units[acount:], indent+"b")
            return self.merge(a, b)
