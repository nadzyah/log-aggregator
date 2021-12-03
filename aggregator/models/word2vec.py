"""Word2vec model"""
import numpy as np
from gensim.models import Word2Vec


class W2VModel():
    """Word2Vec model wrapper"""

    def __init__(self, config=None):
        self.config = config

    def create(self, logs):
        """Create word2vec model

        :param logs: list of normalized log messages (a log message is a list of words)
        """
        self.model = Word2Vec(sentences=list(logs), size=self.config.AGGR_VECTOR_LENGTH, window=self.config.AGGR_WINDOW)

    def get_vectors(self, logs):
        """Return logs as list of vectorized words"""
        self.create(logs)
        vectors = []
        for x in logs:
            temp = []
            for word in x:
                if word in self.model.wv:
                    temp.append(self.model.wv[word])
                else:
                    temp.append(np.array([0]*self.config.AGGR_VECTOR_LENGTH))
            vectors.append(temp)
        return vectors

    def _log_words_to_one_vector(self, log_words_vectors):
        result = []
        log_array_transposed = np.array(log_words_vectors, dtype=object).transpose()
        for coord in log_array_transposed:
            result.append(np.mean(coord))
        return result

    def vectorized_logs_to_single_vectors(self, vectors):
        """Represent log messages as vectors according to the vectors
        of the words in these logs

        :params vectors: list of log messages, represented as list of words vectors
                [[wordvec11, wordvec12], [wordvec21, wordvec22], ...]
        """
        result = []
        for log_words_vector in vectors:
            result.append(self._log_words_to_one_vector(log_words_vector))
        return np.array(result)
