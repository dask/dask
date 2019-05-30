def scan(A, f):
    dsk = dict()
    n = len(A)

    N = 1
    while N < n:
        N *= 2
    for i in range(n):
        dsk[(i, 1, 0)] = A[i]
    for i in range(n, N):
        dsk[(i, 1, 0)] = None

    d = 1
    while d < N:
        for i in range(0, N, 2*d):
            dsk[(i+2*d-1, 2*d, 0)] = f(dsk[(i+d-1, d, 0)], dsk[(i+2*d-1, d, 0)])
        d *= 2

    dsk[(N-1, N, 1)] = None

    while d > 2:
        d //= 2
        for i in range(0, N, 2*d):
            dsk[(i+d-1, d, 1)] = dsk[(i+2*d-1, 2*d, 1)]
            dsk[(i+2*d-1, d, 1)] = f(dsk[(i+2*d-1, 2*d, 1)], dsk[(i+d-1, d, 0)])

    dsk[0] = f(dsk[(1, 2, 1)], dsk[(0, 1, 0)])
    for i in range(2, N, 2):
        dsk[i-1] = dsk[(i+1, 2, 1)]
        dsk[i] = f(dsk[(i+1, 2, 1)], dsk[(i, 1, 0)])
    dsk[n-1] = dsk[(N-1, N, 0)]

    for k in range(n):
        print (dsk[k])

def f(x, y):
    if x is None:
        return y
    if y is None:
        return x
    return x + y

scan([[i] for i in range(16)], f)
