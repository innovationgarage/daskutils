import dask.bag
import dask.distributed
import msgpack
import uuid

def read(filenames):
    def read(infilepath):
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    return filenames.map(lambda x: read(x)).flatten()    

def write(data, filepattern):
    @data.map_partitions
    def write(items):
        filename = filepattern % (uuid.uuid4(),)
        with open(filename, 'wb') as f:
            for msg in items:
                msgpack.dump(msg, f)
        return filename
    return write
