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


while True:
    with env.begin(db=subdb, write=True) as txn:
        cursor = txn.cursor()
        for key, value in cursor.iternext():
            if key is not None:
                event_data = cursor.pop(key)
                print(event_data)
                break
        # close cursor
        cursor.close()
    time.sleep(0.1)

# close database
env.close()
