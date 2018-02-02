import glob, sys, os
from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()
    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for parallel_parse_tweets.py.')
    # main function arguments...
    parser.add_argument('-timelines_glob', type=str, required=True)
    parser.add_argument('-out_filename', type=str)
    parser.add_argument('-out_dir_path', type=str) 
    parser.add_argument('-keep_emoji', action='store_true', help='keep emoji when removing symbols from tweets ')
    parser.add_argument('-single_retweets', action='store_true', help='Have only a signle copy of a retweeted status rather than as many times as it appears in the corpus.')
    parser.add_argument('-no_write', action='store_true', help='Don\'t write to an out file')
    parser.add_argument('-filter_for_en', action='store_true', help='Only select english tweets')
    parser.add_argument('-unnormalized', action='store_true', help='Usually we noramlize text with a normalization dictionary')
    parser.add_argument('-child_process', action='store_true')
    parser.add_argument('-parallel', action='store_true', help='Use multiple cores to process topics in parallel.')
    parser.add_argument('-max_ncpus', type=int, default=8, help='maximum number of cores to run in parallel')

    opts = parser.parse_args()


def parse_file(file, opts):
    sys.stderr.write("EXECUTING: %s\n" % file)
    sys.stderr.flush()
    file_dir = os.path.dirname(os.path.realpath('__file__'))
    report_fname = os.path.basename('%s_parse_output.txt' % file)
    if not opts.out_dir_path:
        report_path = os.path.join(file_dir, '../data/parsed_timelines', report_fname)
    else:
        report_path = os.path.join(file_dir, opts.out_dir_path, report_fname)

    cmdline = 'nice python write_tweet_corpus_to_files.py'
    cmdline += ' -timelines_glob %s' % file

    if opts.out_filename:
        cmdline += ' -out_filename %s' % (opts.out_filename)
    if opts.out_dir_path:
        cmdline += ' -out_dir_path %s' % (opts.out_dir_path)
    if opts.keep_emoji:
        cmdline += ' -keep_emoji'
    if opts.single_retweets:
        cmdline += ' -single_retweets'
    if opts.filter_for_en:
        cmdline += ' -filter_for_en'
    if opts.unnormalized:
        cmdline += ' -unnormalized'

    cmdline += ' &> %s; ' % report_path

    #cmdline += 'python send_email.py -filename %s -subject %s;' % (fname, fname)
    
    os.system(cmdline)
    sys.stderr.write("COMPLETED: %s\n" % file)
    sys.stderr.flush()    

if opts.parallel:
    with ThreadPoolExecutor(max_workers=opts.max_ncpus) as executor:
        timelines = glob.glob(opts.timelines_glob)
        result = executor.map(parse_file, timelines, [opts]*len(timelines))
        for r in result:
            pass
else:    
    for file in glob.glob(opts.timelines_glob):
        sys.stderr.write("EXECUTING: %s\n" % file)
        parse_file(file, opts)
        sys.stderr.write("COMPLETED: %s" % file)