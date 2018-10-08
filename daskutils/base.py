import dask.bag
import dask.distributed

def glom(bag):
    return bag.map_partitions(lambda i: [i])

def enumerate(data):
    return dask.bag.zip(data, db.range(data.count().compute(), data.npartitions))

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
 
