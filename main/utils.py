import numpy as np

class SimilarityUtil(object):
    @classmethod
    def cos(cls, v1, v2):
        # print 'V1: ', v1
        # print 'V2: ', v2
        return np.dot(v1, v2) / (np.sqrt(np.dot(v1, v1)) * np.sqrt(np.dot(v2, v2)))

class RMSEUtil(object):
    @classmethod
    def rmse(cls):
        pass