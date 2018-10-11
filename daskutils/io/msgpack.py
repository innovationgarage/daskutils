import dask.bag
import dask.distributed
import daskutils.base
import msgpack

def read(filenames):
    def read(infilepath):
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    return filenames.map(lambda x: read(x)).flatten()    

def write(data, filepattern):
    data = daskutils.base.enumerate(daskutils.base.glom(data))
    @data.map
    def write(item):
        lines, idx = item
        filename = filepattern % (idx,)
        with open(filename, 'wb') as f:
            for msg in lines:
                msgpack.dump(msg, f)
        return filename
    return write
