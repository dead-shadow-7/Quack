import re
import nltk

# Download required NLTK data (safe to call multiple times)
nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)

from nltk.stem import SnowballStemmer      # better than PorterStemmer
from nltk.corpus import stopwords

stemmer    = SnowballStemmer("english")
stop_words = set(stopwords.words("english"))


def process_text(text: str) -> list[str]:
    """
    Lowercase → tokenize → remove stopwords & short tokens → stem.
    Returns a flat list of stems (duplicates kept for TF calculation).
    """
    text   = text.lower()
    tokens = re.findall(r"[a-z0-9]+", text)
    tokens = [
        stemmer.stem(w)
        for w in tokens
        if w not in stop_words and len(w) > 1
    ]
    return tokens