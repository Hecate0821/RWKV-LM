data_size = 2857897120
CTX_LEN = 1024
def is_prime(n):
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True
n_chunk = int(data_size // CTX_LEN) - 1
for i in range(n_chunk, 0, -1):
    if i % 3 == 2:
        if is_prime(i):
            print(f"\n### magic_prime = {i} (for ctxlen {CTX_LEN})")
            break