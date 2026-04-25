import sys, random, time
n = int(sys.stdin.read().strip())
start = time.time()
A = [[random.random() for _ in range(n)] for _ in range(n)]
B = [[random.random() for _ in range(n)] for _ in range(n)]
C = [[0.0] * n for _ in range(n)]
for i in range(n):
    for k in range(n):
        aik = A[i][k]
        for j in range(n):
            C[i][j] += aik * B[k][j]
checksum = sum(C[i][j] for i in range(n) for j in range(n))
elapsed = time.time() - start
print(f"{checksum:.6f}")
