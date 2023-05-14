# Refer to this user manual: https://lmdb.readthedocs.io/en/release/#lmdb

import time
import lmdb

env = lmdb.open("./db/test2", map_size=250 * 1024 * 1024, max_dbs=2)

# how to begin transaction with default database
with env.begin() as txnmain:
    cursor = txnmain.cursor()
    for key, value in cursor.iternext():
        print(key, value)
time.sleep(1)

# how to begin transaction with the database activating duplicate key
subdb = env.open_db(b"ens", dupsort=True)
with env.begin(db=subdb, write=True) as txn:
    # how to get keys..
    for idx in range(1000):
        txn.put(str(idx).encode(), str(idx * 10).encode())
    myList = [key for key, _ in txn.cursor()]
    print(myList)

    # how to put key-value data with the same key
    txn.put(b"999", b"111", db=subdb)
    txn.put(b"999", b"112", db=subdb)
    txn.put(b"999", b"113", db=subdb)

    # how to interate key-value data
    cursor = txn.cursor()
    for key, value in cursor.iternext():
        print(key, value)

    # how to get only first value of this key
    print(cursor.get(b"999"))

    # how to delete a key-value data
    txn.delete(b"999", b"112")

    # how to get the all values of this key
    print(cursor.getmulti([b"999"], dupdata=True))

    # how to get stats of this databalse
    print(txn.stat())

    # test first()
    print("test first()")
    print(cursor.first())
    print(cursor.first())
    print(cursor.first())

    # close cursor
    cursor.close()

# close database
env.close()
