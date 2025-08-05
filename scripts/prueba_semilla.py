import random

def gauss_con_limite_inferior(mu, sigma, n_elementos, limite_inferior):
    res = []
    for n in range(n_elementos):
        while True:
            x = random.gauss(mu, sigma)
            if x >= limite_inferior:
                res.append(x)
                break
    return res

random.seed(1)
res1 = gauss_con_limite_inferior(1,1,10,0)
res2 = gauss_con_limite_inferior(1,1,10,0)

print(res1)
print(res2)
