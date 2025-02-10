import math
import random


def formula1(greek_lambda):
    greek_ksi = random.random()
    x = - (1 / greek_lambda) * math.log(greek_ksi)
    return x

def formula2(greek_sigma, a):
    greek_mu = sum([random.random() for i in range(12)]) - 6
    x = greek_sigma * greek_mu + a
    return x

def formula3():
    a = 5 ** 13
    c = 2 ** 31
    return