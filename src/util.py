import asyncio

# A brainless simple wrapper that waits on all the future in the list to complete
# and return the results
def wait_futures(futures):
    res = []
    # print("Job to be waited on " + str(len(futures)), flush=True)
    # We loop over a copy of the futures to avoid concurrent modification
    last_updated = len(futures)
    while len(futures) != 0:
        for future in list(futures):
            if future.done():
                try:
                    # print("Job result " + str(future.result()))
                    res.append(future.result())
                except asyncio.CancelledError:
                    # ignore
                    dummpy = -1
                futures.remove(future)
        if (len(futures) != last_updated):
            # print("Jobs remaining: " + str(len(futures)))
            last_updated = len(futures)
    
    return res