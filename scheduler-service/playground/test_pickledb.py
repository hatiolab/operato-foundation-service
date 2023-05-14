import pickledb
import time

db = pickledb.load("test.db", True)

start = time.time()
for i in range(100):
    db.set(str(i), str(i * 10))
end = time.time()
print(f"Elapsed time: {end-start} (sec)")

# while True:
#     aaaa = db.getall()
#     time.sleep(0.001)
