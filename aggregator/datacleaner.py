import re
import pandas as pd
import datetime

class DataCleaner:
    """Data cleaning utility functions."""

    @classmethod
    def _clean_message(cls, line):
        """Remove all none alphabetical characters from message strings."""
        words = list(re.findall("[a-zA-Z]+", line))
        return words

    @classmethod
    def _preprocess(cls, data):
        """Provide preprocessing for the data before running it through W2V and SOM."""
        def to_str(x):
            """Convert all non-str lists to string lists for Word2Vec."""
            ret = " ".join([str(y) for y in x]) if isinstance(x, list) else str(x)
            return ret

        for col in data.columns:
            if col == "message":
                data[col] = data[col].apply(cls._clean_message)
            else:
                data[col] = data[col].apply(to_str)

        data = data.fillna("EMPTY")
