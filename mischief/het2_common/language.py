import stemming.porter2
from string import punctuation, maketrans
from itertools import chain

PUNCTUATION = maketrans(punctuation, ' '*len(punctuation))

def stem(word):
    return stemming.porter2.stem(word)

def stem_words(words):
    # TODO: see if we need the original capitalization
    return [stem(word.lower()) for word in words]

def stem_phrase(phrase):
    words = phrase.translate(PUNCTUATION).split()
    return stem_words(words)
