# write_w2v_validation_text.py


if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for twain_w2v.py.')
    # main function arguments...
    parser.add_argument('-validation_components', type=str, required=True)
    parser.add_argument('-out_filename', type=str, default="validation_" + "datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')")

    opts = parser.parse_args()

with open(opts.validation_components) as f:
    components = f.read().splitlines()

# separate into categories and wordpairs for ananlogies
# categories are demarcated by a ':', e.g., : synonyms
# cat feline
# dog canine
# keys are categories, values are a list of word pairs
components_dict = {}
for c in components:
    if c[0] == ':':
        cat = c
        components_dict[cat] = []
    else:
        components_dict[cat] += [c]

# write the output file
# categories are again demarcated with ':'
# wordpairs within the categories should be permuted into all remaining permutations
# wordpairs are space delimited both within and between pairs
with open(opts.out_filename + '.txt', 'w') as f:
    for cat in components_dict:
        f.write('%s\n'%cat)
        for i in range(len(components_dict[cat])-1):
            for j in range(i+1,len(components_dict[cat])):
                f.write("%s %s\n" % (components_dict[cat][i],components_dict[cat][j]))

