import nltk
nltk.download("punkt")
nltk.download("stopwords")

import re
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

stemmer = PorterStemmer()
stop_words = set(stopwords.words("english"))

def process_text(text):
    text = text.lower()
    tokens = re.findall(r'[a-z0-9]+', text)  
    tokens = [stemmer.stem(w) for w in tokens if w not in stop_words and len(w) > 1]
    return tokens