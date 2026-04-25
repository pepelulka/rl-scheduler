import sys, time
n = int(sys.stdin.read().strip())
start = time.time()
sieve = bytearray([1]) * (n + 1)
sieve[0] = sieve[1] = 0
i = 2
while i * i <= n:
    if sieve[i]:
        step = len(sieve[i*i::i])
        sieve[i*i::i] = b'\x00' * step
    i += 1
count = sum(sieve)
elapsed = time.time() - start
print(f"{count}")
