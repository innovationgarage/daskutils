import dask.bag

def median(bag, epsilon=1e-30):
     @dask.delayed
     def median(bmin, bmax, bcount):
          step = (bmax - bmin) / 2.
          proposal = bmin + step
          while step > epsilon:
               largercount = bag.filter(lambda x: x > proposal).count().compute()
               if largercount == bcount // 2:
                    return proposal
               step /= 2
               if largercount > bcount // 2:
                    proposal += step
               else:
                    proposal -= step
          return None
     return median(bag.min().to_delayed(), bag.max().to_delayed(), bag.count().to_delayed())
