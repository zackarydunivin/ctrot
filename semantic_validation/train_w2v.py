import gensim.models as gs
from collections import defaultdict
from pprint import pprint
import glob, gzip
import pickle 
_gnews_path  = '../models/w2v/GoogleNews-vectors-negative300.bin'
_ghent_path = '../models/w2v/word2vec_twitter_model.bin'

class SentenceGenerator(object):
    def __init__(self, dirname):
        self.dirname = dirname
 
    def __iter__(self):
        for gz in glob.glob(self.dirname):
            with gzip.open(gz, 'rb') as f:
                for line in f:
                    yield line.decode("utf8").replace(",\n","").split()

def check_w2v_model_for_missing_vocab(sentences,model_path):
        w2v = gs.KeyedVectors.load_word2vec_format(model_path, binary=True, unicode_errors='ignore')
        w2v['alt-right']
        sys.exit()
        missing = defaultdict(int)
        """
        words = ['huma','cuck','limabean','alagash','🐸']
        for word in words:
            print(word)
            try:
                w2v[word]
                print(w2v.most_similar(positive=[word]),topn=10)
            except KeyError:
                continue
        """

        for s in sentences:
            for token in s:
                try:
                    w2v[token]
                except KeyError:
                    missing[token] += 1


        pprint(dict(missing))

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for manual_twitter_scrape.py.')
    # main function arguments...
    parser.add_argument('-timelines_glob', type=str, required=True)

    opts = parser.parse_args()

    sentences = SentenceGenerator(opts.timelines_glob) # a memory-friendly iterator
    model = gensim.models.Word2Vec(sentences, size=300, min_count=10)
    with ('my_model.p', 'b') as f:
        pickle.dump(model,f)


