from uuid import uuid4
import random
import time

import oracledb as odb


def select_test():
    try:
        dbconn = odb.connect(
            user="SCHEDULER", password="Abcd1234", dsn="34.64.79.183/XE"
        )
        dbconn.autocommit = True

        dbconn.close()
    except Exception as ex:
        print(ex)
        dbconn and dbconn.close()


if __name__ == "__main__":
    select_test()
