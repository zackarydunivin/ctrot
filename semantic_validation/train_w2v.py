from collections import defaultdict
from pprint import pprint
import glob, gzip
import pickle, datetime, time, os, sys
import gensim
from gensim.models import Word2Vec, KeyedVectors
import pprint

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

def model_accuracy_report(accuracy):
    report = []
    for d in accuracy:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        t_count = len(d['correct'])
        f_count = len(d['incorrect'])
        try:
            fraction = t_count/(f_count+t_count)
        except ZeroDivisionError:
            fraction = 0
        report.append('%s  INFO  %s  %.2f%%  (%d,%d)' % (timestamp, d['section'], fraction,t_count,f_count))
    return report

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for twain_w2v.py.')
    # main function arguments...
    parser.add_argument('-timelines_glob', type=str)
    default_model_fname = 'w2v_model_' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S')
    parser.add_argument('-model_filename', type=str, default=default_model_fname)
    parser.add_argument('-load_model', action='store_true', help='a w2v .bin to be loaded and validated')
    opts = parser.parse_args()
    if opts.load_model:
        sys.stdout.write("Loading model %s . . . " % opts.model_filename)
        if os.path.splitext(opts.model_filename)[1] == '.p':
            with open(opts.model_filename,'rb') as f:
                model = pickle.load(f)
        elif os.path.splitext(opts.model_filename)[1] == '.txt':
            model = Word2Vec.load_word2vec_format(opts.model_filename, binary=False, unicode_errors='ignore')
        # .bin or .bin.gz
        else:
            model = KeyedVectors.load_word2vec_format(opts.model_filename, binary=True, unicode_errors='ignore')
        sys.stdout.write("OK.\n")
    else:
        sys.stdout.write("Training model %s . . . " % opts.model_filename)
        sentences = SentenceGenerator(opts.timelines_glob) # a memory-friendly iterator
        model = Word2Vec(sentences, size=300, min_count=10)
        file_dir = os.path.dirname(os.path.realpath('__file__'))
        model_fname = os.path.splitext(os.path.basename(opts.model_filename))[0]
        model_dir =  os.path.join(file_dir, 'w2v_validation/%s' % model_fname + '.p')
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        model_path = os.path.join(model_dir, model_fname) 
        model.save(model_path)
        sys.stdout.write("OK.\n")

    standard_report = model_accuracy_report(model.accuracy('w2v_validation/standard_validation.txt'))
    wn_report = model_accuracy_report(model.accuracy('w2v_validation/wn_validation.txt'))
    file_dir = os.path.dirname(os.path.realpath('__file__'))
    model_fname = os.path.splitext(os.path.basename(opts.model_filename))[0]
    report_dir =  os.path.join(file_dir, 'w2v_validation/%s' % model_fname)
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    report_fname = os.path.join(report_dir, model_fname +'_accuracy_report.txt') 
    with open(report_fname, 'w') as f:
        for line in standard_report + wn_report:
            f.write("%s\n" % line)

    full_report_fname = os.path.join(report_dir, model_fname +'_full_report.txt')         
    with open(full_report_fname, 'w') as f:
        pprint.pprint(model.accuracy('w2v_validation/wn_validation.txt'), f)

