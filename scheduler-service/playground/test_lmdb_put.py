# Refer to this user manual: https://lmdb.readthedocs.io/en/release/#lmdb

import time
import lmdb

env = lmdb.open(
    "../db/test2",
    map_size=250 * 1024 * 1024,
    max_dbs=2,
)

# how to begin transaction with the database activating duplicate key
subdb = env.open_db(b"ens", dupsort=True)


for idx in range(1000000):
    with env.begin(db=subdb, write=True) as txn:
        # how to get keys..
        txn.put(str(idx).encode(), str(idx * 10).encode())
    time.sleep(1)
    print(f"wrote idx {idx}")

# close database
env.close()
