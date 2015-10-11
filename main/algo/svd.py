import numpy as np

class SVDAlgorithm(object):
    @classmethod
    def process(cls, A):
        u, s, v = np.linalg.svd(A)
    return u, s, v