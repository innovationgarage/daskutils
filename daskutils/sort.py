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
import subprocess
import base64

@contextlib.contextmanager
def worker_client(*arg, **kw):
    started = False
    try:
        with dask.distributed.worker_client(*arg, **kw) as c:
            started = True
            yield c
    except ValueError:
        if started:
            raise
        else:
            yield None
        
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

def existing_base(path):
    while path != "/":
        if os.path.exists(path):
            return path
        path = os.path.split(path)[0]
    assert False

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
    
    @classmethod
    @dask.delayed
    def merge2(cls, a, b):
        with worker_client() as client:
            return a.merge(b).compute()

    def append(self, other):
        # Append and create a balanced tree
        if other is None:
            return self
        if self.minval < other.minval:
            a, b = self, other
        else:
            a, b = other, self

        def build_tree(sort_units):
            count = len(sort_units)
            if count <= 1:
                return sort_units[0]
            else:
                acount = count // 2
                a = build_tree(sort_units[:acount])
                b = build_tree(sort_units[acount:])
                return self.construct(
                    minval=a.minval,
                    maxval=b.maxval,
                    count=a.count + b.count,
                    a=a,
                    b=b)
        return build_tree(list(a.flatten()) + list(b.flatten()))
    
    @dask.delayed
    def merge(self, other):
        # print("Merging %s[%s,%s] and %s[%s,%s]" % (self.count, self.minval, self.maxval, other.count, other.minval, other.maxval))
        with worker_client() as client:
            if other is None:
                return self
            elif self.maxval < other.minval:
                return self.append(other)
            elif other.maxval < self.minval:
                return other.append(self)
            elif self.data and other.data:
                return self.merge_simple(other).compute()
            elif self.data:            
                return other.merge(self).compute()
            else:
                return self.merge_splitted(other.split(self.b.minval)).compute()

    @dask.delayed
    def merge_splitted(self, items):
        a, b = items
        @dask.delayed
        def construct(a, b):
            return a.append(b)
        with worker_client() as client:
            return construct(self.a.merge(a), self.b.merge(b)).compute()
        
    @dask.delayed
    def split(self, value):
        with worker_client() as client:
            if self.maxval < value:
                return self, None
            elif self.minval > value:
                return None, self
            elif self.data:
                return self.split_simple(value).compute()
            else:
                if value < self.b.minval:
                    a, b = self.a.split(value).compute()
                    return a, self.b.append(b)
                else:
                    a, b = self.b.split(value).compute()
                    return self.a.append(a), b

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
                subprocess.check_output(["tail", "-n", "1", splitname + "a"]).split(b"||", 1)[1]))

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
        self._key = key
        self.partition_size = partition_size

    def key(self, item):
        itemkey = self._key(item)
        if not isinstance(itemkey, (tuple, list)):
            itemkey = (itemkey,)
        return itemkey
    
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
        keytypes = ["n" if type(keypart) in (int, float) else ""
                    for keypart in self.itemkey(data.take(1)[0])]
        self.keydef = ",".join("%s%s" % (idx+1, keytype) for (idx, keytype) in enumerate(keytypes))

        filenames = data.map_partitions(self.repartition_and_save)
        sort_units = [dask.delayed(sort_unit) for sort_unit in filenames.map(self.sort_sort_unit).compute()]

        sort_unit = self.merge_sort([dask.delayed(sort_unit) for sort_unit in sort_units])
        sort_unit = sort_unit.compute()

        data = dask.bag.from_sequence(sort_unit.flatten(), 1)
        
        @data.map_partitions
        def data(part):
            return part[0].read()
        
        return data

    def dumps(self, item):
        return base64.b64encode(msgpack.dumps(item))

    def loads(self, line):
        try:
            return msgpack.loads(base64.b64decode(line), raw=False)            
        except Exception as e:
            raise Exception("%s: %s" % (e, repr(line)))
        
    def itemkey(self, item):
        itemkey = self.key(item)
        if not isinstance(itemkey, (tuple, list)):
            itemkey = (itemkey,)
        return itemkey
    
    def formatkey(self, itemkey):
        return b"|".join(str(keypart).encode("utf-8") for keypart in itemkey)

    def repartition_and_save(self, data):
        partitionid = self.tempfile()
        sort_units = []
        f = None
        p = 0
        s = None
        for idx, item in enumerate(data):
            part = idx // self.partition_size
            if f is None or part != p:
                if f: f.close()
                p = part
                filename = self.tempfile(partitionid)
                f = open(filename, 'wb')
                s = self.sort_unit(minval=None, maxval=None, count=0, data=filename)
                sort_units.append(s)
            itemkey = self.key(item)
            if s.minval is None or s.minval > itemkey:
                s.minval = itemkey
            if s.maxval is None or s.maxval < itemkey:
                s.maxval = itemkey
            s.count += 1
            f.write(self.formatkey(itemkey))
            f.write(b"||")
            f.write(self.dumps(item))
            f.write(b"\n")
        if f: f.close()
        return sort_units
    
    def sort_sort_unit(self, sort_unit):
        outfilename = self.tempfile(sort_unit.data, op="sort")
        subprocess.check_call(["sort", "-k", self.keydef, "-o", outfilename, sort_unit.data])
        return self.sort_unit(minval=sort_unit.minval, maxval=sort_unit.maxval, count=sort_unit.count, data=outfilename)
    
    def sort_unit(self, *arg, **kwarg):
        return SortUnit(self, *arg, **kwarg)
    
    def merge_sort(self, sort_units, indent='>'):
        count = len(sort_units)
        if count <= 1:
            return sort_units[0]
        else:
            acount = count // 2
            a = self.merge_sort(sort_units[:acount], indent+"a")
            b = self.merge_sort(sort_units[acount:], indent+"b")
            return SortUnit.merge2(a, b)
