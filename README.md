# daskutils

Utilities on top of [dask](https://dask.org/)

# daskutils.io.msgpack

Utilities to read and write dask bags to/from files on disk in msgpack format. They assume a one to one correspondence
between file on disk and partitions.

    dask_bag_of_filepaths = daskutils.io.msgpack.write(dask_bag_of_data, "/path/to/files/myfile-%s.msgpack")
    
    dask_bag_of_data = daskutils.io.msgpack.read(dask_bag_of_filepaths)

# daskutils.base

## daskutils.base.glom

Wraps the content of each partition in a list. After this, each partition contains a single item,
the list of all the former items of that partition.

## daskutils.base.enumerate

Enumerates the items in a dask bag. The resulting bag contains tuples of (item, index).

## daskutils.base.glom_enumerate

Equivalent to enumerate(glom(bag))) but slightly faster.

## daskutils.base.filter_count(bag, filter)

Equivalent to bag.filter(filter).count() but slightly faster.

# daskutils.math.median(bag)

Calculates an aproximation of the median value of a bag.
The content of the bag does not have to be sorted. Performance is O(log n). Half the items in the bag are guaranteed to be
greater and half smaler, than this aproximate value. However, the value does not have to be at equal distance
from the two closest items.

# daskutils.sort.MergeSort

This is a O(n * log(n) / k) standard merge sort implementation (where k is number of nodes) for dask bags.

    sorter = daskutils.sort.MergeSort("/path/to/tmpdir", lambda item: iten["sortkey"], partition_size=2000)
    sorted_dask_bag = sorter.sort(unsorted_dask_bag)
