# convert_w2v_model.py
from gensim.models import Word2Vec, KeyedVectors
import os



if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for twain_w2v.py.')
    # main function arguments...
    parser.add_argument('-model_filename', type=str)
    opts = parser.parse_args()
    #sys.stdout.write("Loading model %s . . . " % opts.model_filename)
    model_fname, model_ext = os.path.splitext(opts.model_filename)
    if model_ext == '.p':
        model = Word2Vec.load(opts.model_filename)
        model.save_word2vec_format(model_fname + '.bin', binary=True)
    elif model_ext == '.txt':
        model = KeyedVectors.load_word2vec_format(opts.model_filename, binary=False, unicode_errors='ignore')
    # .bin or .bin.gz
    else:
        model = KeyedVectors.load_word2vec_format(opts.model_filename, binary=True, unicode_errors='ignore')

