import daskutils.math
import daskutils.base
import dask.bag

def sort_partitions(bag):
     @bag.map_partitions
     def res(lines):
          return sorted(lines, key=lambda k, v: k)
     return res

def split_sort(tosort):
     if tosort.npartitions < 1:
          return sort_partitions(tosort)
     else:
          m = daskutils.math.median(tosort)
          a = tosort.filter(lambda item: item > m)
          b = tosort.filter(lambda item: item <= m)
          return dask.bag.concat(split_sort(a), split_sort(b))

def merge_sort(tosort):
     if tosort.npartitions < 1:
          return sortPartitions(tosort).repartition(2 * tosort.npartitions)
     else:
          daskutils.base.bag_glom(tosort)
          data.count().compute()
          
