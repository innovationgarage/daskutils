import dask.bag
import dask.distributed

def glom(bag):
    return bag.map_partitions(lambda i: [list(i)])

def enumerate(data):
    if data.npartitions == 0:
        return data
    return dask.bag.zip(data, dask.bag.range(data.count().compute(), data.npartitions))

def glom_enumerate(data):
    if data.npartitions == 0:
        return data
    data = glom(data)
    return dask.bag.zip(data, dask.bag.range(data.npartitions, data.npartitions))

def bag_glom(bag):
    return bag.map_partitions(lambda i: [dask.bag.from_sequence(i)])

def filter_count(bag, flt):
     def partition_count(part):
          count = 0
          for item in part:
               if flt(item): count += 1
          return count
     return bag.reduction(partition_count, sum)

def bag_group_by(bag, grouper):
    return bag.foldby(
        grouper,
        lambda a, b: a + (b,), (),
        lambda a, b: dask.bag.concat([a, dask.bag.from_sequence(b)]),
        dask.bag.from_sequence([]))
 
