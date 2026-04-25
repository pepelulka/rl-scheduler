import sys, random, time
n = int(sys.stdin.read().strip())
start = time.time()
inside = 0
for _ in range(n):
    x = random.random()
    y = random.random()
    if x*x + y*y <= 1.0:
        inside += 1
pi = 4.0 * inside / n
elapsed = time.time() - start
print(f"{pi:.6f}")
