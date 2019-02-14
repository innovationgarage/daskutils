import dask.bag
import dask.distributed
import os.path
import uuid
import msgpack
import itertools
import socket
import contextlib
import subprocess

@contextlib.contextmanager
def debugopen(filename, *arg, **kw):
    try:
        with open(filename, *arg, **kw) as f:
            yield f
    except FileNotFoundError:
        base = existing_base(filename)
        hostname = socket.gethostname()
        #with open("/etc/host-hostname") as f:
        #    hostname = f.read().strip()
        raise FileNotFoundError("%s:%s: only %s exists, containing %s" % (hostname, filename, base, os.listdir(base)))

def existing_base(path):
    while path != "/":
        if os.path.exists(path):
            return path
        path = os.path.split(path)[0]
    assert False

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
        assert not isinstance(minval, float)
        assert not isinstance(maxval, float)
        
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
        
        splitline = self.mergesort.formatkey(value).decode("utf-8") + "||SPLIT"

        splitname = os.path.join(tempdir, str(uuid.uuid4()))
        subprocess.check_call(["bash", "-c", """
          sort -k %(keydef)s -m "%(data)s" <(echo "%(split)s") |
            sed '1,/%(split)s/w '>(head -n -1 > "%(splitname)sa")'\n/%(split)s/,$w '>(tail -n +2 > "%(splitname)sb")'' > /dev/null
        """ % {
            "keydef": self.mergesort.keydef,
            "data": self.data,
            "split": splitline,
            "splitname": splitname
        }])
        amaxval = self.mergesort.itemkey(
            self.mergesort.loads(
                subprocess.check_output(["tail", "-n", "1", splitname + "a"]).split(b"||")[1]))

        alen = int(subprocess.check_output(["wc", "-l", splitname + "a"]).split(b" ")[0])
        blen = int(subprocess.check_output(["wc", "-l", splitname + "b"]).split(b" ")[0])
        
        aout = self.construct(minval=self.minval, maxval=amaxval, count=alen, data=splitname + "a")
        bout = self.construct(minval=value, maxval=self.maxval, count=blen, data=splitname + "b")

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

        mergedname = os.path.join(tempdir, str(uuid.uuid4()))
        subprocess.check_call(["sort", "-k", self.mergesort.keydef, "-o", mergedname, "-m", self.data, other.data])

        subprocess.check_call(["split", "-n", "l/2", mergedname, mergedname])

        aout = self.construct(count=0, data=mergedname + "aa")
        bout = self.construct(count=0, data=mergedname + "ab")        

        for out in (aout, bout):
            with debugopen(out.data, 'rb') as f:
                for line in f:
                    if out.minval is None:
                        out.minval = self.mergesort.itemkey(self.mergesort.loads(line.split(b"||", 1)[1]))
                    out.count += 1
                out.maxval = self.mergesort.itemkey(self.mergesort.loads(line.split(b"||", 1)[1]))

        if bout.count == 0:
            return aout

        assert isinstance(aout.minval, tuple)
        assert isinstance(bout.minval, tuple)
        assert isinstance(aout.maxval, tuple)
        assert isinstance(bout.maxval, tuple)
        
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
            for line in f:
                try:
                    yield self.mergesort.loads(line.split(b"||", 1)[1])
                except:
                    print(self.data)
                    print(line)
                    raise
        
class MergeSort(object):
    def __init__(self, tempdir, key=lambda a: a, partition_size=2000):
        self.tempdir = tempdir
        self.key = key
        self.partition_size = partition_size
        
    def sort(self, data):
        keytypes = ["n" if type(keypart) in (int, float) else ""
                    for keypart in self.itemkey(data.take(1)[0])]
        self.keydef = ",".join("%s%s" % (idx+1, keytype) for (idx, keytype) in enumerate(keytypes))

        sort_units = data.map_partitions(self.partition_to_sort_unit).compute()
        sort_unit = self.merge_sort([dask.delayed(sort_unit) for sort_unit in sort_units])
        sort_unit = sort_unit.compute()

        data = dask.bag.from_sequence(sort_unit.flatten(), 1)
        
        @data.map_partitions
        def data(part):
            return part[0].read()
        
        return data

    def dumps(self, item):
        return msgpack.dumps(item).replace(b"\\", br"\\").replace(b"\n", br"\n")

    def loads(self, line):
        return msgpack.loads(line.strip(b"\n").replace(br"\n", b"\n").replace(br"\\", b"\\"), raw=False)            
        
    def itemkey(self, item):
        itemkey = self.key(item)
        if not isinstance(itemkey, (tuple, list)):
            itemkey = (itemkey,)
        return itemkey
    
    def formatkey(self, itemkey):
        return b"|".join(str(keypart).encode("utf-8") for keypart in itemkey)
        
    def partition_to_sort_unit(self, data):
        filename = os.path.join(self.tempdir, str(uuid.uuid4()))

        minval = None
        maxval = None
        count = 0
        with debugopen(filename, 'wb') as f:
            for item in data:
                count += 1
                itemkey = self.key(item)
                if not isinstance(itemkey, (tuple, list)):
                    itemkey = (itemkey,)
                if minval is None:
                    minval = itemkey
                if maxval is None or itemkey > maxval:
                    maxval = itemkey
                f.write(self.formatkey(itemkey))
                f.write(b"||")
                f.write(self.dumps(item))
                f.write(b"\n")
                
        sortedname = filename + ".sorted"
        subprocess.check_call(["sort", "-k", self.keydef, "-o", sortedname, filename])
        
        return [self.sort_unit(minval=minval, maxval=maxval, count=count, data=sortedname)]
    
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
