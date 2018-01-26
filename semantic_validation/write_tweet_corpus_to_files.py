# -*- coding: utf-8 -*-
import os, sys, glob, gzip, time, re
from tqdm import tqdm
from nltk.tokenize.casual import TweetTokenizer
import json
from cucco import Cucco
from utils.emnlp_dict import emnlp_dict
from collections import defaultdict
from pprint import pprint
import unicodedata
from utils.reassembler_dict import emoji_reassembler_dict
from nltk.corpus import stopwords
from emoji import UNICODE_EMOJI
english_stopwords = stopwords.words("english")

#EMOJI_REGEX = re.compile('([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])')
#EMOJI_FLAG_REGEX =  re.compile('([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF]|([\U0001F1E6-\U0001F1FFU+1F9EF]))')
FLAG_REGEX = re.compile('([\U0001F1E6-\U0001F1FF])')
SMALL_LETTER_TAG_REGEX = re.compile('([\U000E0061-\U000E007F])')

def remove_stop_words(text):
        return ' '.join([word for word in text.split() if word not in english_stopwords])

def gzip_timeline_generator(filename):
    with gzip.open(gz, 'rb') as f:
        for line in f:
            yield line

cucco = Cucco()
def cucco_normalizations(text,keep_emoji=True):
    normalizations = [
    #'remove_stop_words',
    'remove_accent_marks',
    ('replace_urls', {'replacement': ' '}),
    #('replace_characters', {'characters':{'\'','ʼ','’'},'replacement':''}),
    # leaving @ allows us to remove handles with TweetTokineizer
    ('replace_punctuation', {'replacement': ' ','excluded':''}),

    ]

    def replace_symbols(text, form='NFD', excluded=None, replacement=''):
        """Replace symbols in text.
        Removes symbols from input text or replaces them with a
        string if specified.
        Args:
            text: The text to be processed.
            form: Unicode form.
            excluded: Set of unicode characters to exclude.
            replacement: New text that will replace symbols.
        Returns:
            The text without symbols.
        """

        
        if excluded is None:
            excluded = set()

        categories = set(['Mn', 'Sc', 'Sk', 'Sm', 'So', 'Pf', 'Pi'])
        return ''.join(c if unicodedata.category(c) not in categories or FLAG_REGEX.search(c) or c in excluded or c in UNICODE_EMOJI
                       else replacement for c in text)#unicodedata.normalize(form, text))
    
    text = remove_stop_words(text)
    normalized = cucco.normalize(text, normalizations)

    if keep_emoji:
        rel_pol_sym = ['☧','☨','☩','☫','☬','☭','☯']
        dingbats = ['✙','✚','✛','✜','✝','✞','✟','✠','✡']
        others_symbols = ['卍','卐','࿗','࿘','ᛊ','ᛋ']
        zwj = ['‍']
        """
         2626 ☦ ORTHODOX CROSS
         2627 ☧ CHI RHO = Constantine's cross, Christogram → 2CE9 ⳩ coptic symbol khi ro
         2628 ☨ CROSS OF LORRAINE
         2629 ☩ CROSS OF JERUSALEM → 1F70A alchemical symbol for vinegar
         262A ☪ STAR AND CRESCENT
         262B ☫ FARSI SYMBOL = symbol of Iran (1.0)
         262C ☬ ADI SHAKTI = Gurmukhi khanda
         262D ☭ HAMMER AND SICKLE
         262E ☮ PEACE SYMBOL
         262F ☯ YIN YANG → 0FCA ࿊ Tibetan symbol nor bu nyis -khyil

         719 ✙ Outlined Greek cross
         271A ✚ Heavy Greek cross
         271B ✛ Open center cross
         271C ✜ Heavy open center cross
         271D ✝ Latin cross
         271E ✞ Shadowed white Latin cross
         271F ✟ Outlined Latin cross
         2720 ✠ Maltese cross
         2721 ✡ Star of David
         534D 卍 Left-Facing Ideograph Swastika
         5350 卐 Right-Facing Ideograph Swastika
         0FD7 ࿗ Right-Facing Svasti Sign with Dots Swastika
         0FD8 ࿘ Left-Facing Svasti Sign with Dots Swastika
         16CA ᛊ Runic Letter Sowilo S
         16CB ᛋ Runic Letter Sigel Long-Branch-Sol S
        """
        excluded = rel_pol_sym + dingbats + others_symbols + zwj
        excluded = set(excluded)
        normalized = replace_symbols(normalized,excluded=excluded)
    else:
        normalized = cucco.replace_symbols(normalized)

    return normalized



def reassemble_emoji_from_utf8_string(text):
    text_len = len(text) 
    chars = [body[i:i+4] for i in range(0,text_len,4)] 
    return reassemble_emoji_from_utf8_char_list(chars)

def reassemble_emoji_from_utf8_char_list(chars):
    # code borrowed from https://github.com/fionapigott/emoji-counter/blob/master/parse_utf32.py 
    j = 0
    reassembled = []
    num_chars = len(chars)
    while j < num_chars:
        char = chars[j].encode('utf-8')
        # if not it's an emoji
        if not char in emoji_reassembler_dict:
            print(char)
            reassembled.append(char.decode())
        # it's an emoji and it's in the dict
        else:
            # set emoji_found to the character we just read
            emoji_found = char
            # get the info from the emoji dict
            char_info = emoji_reassembler_dict[char]
            # look for skintone modifier
            look_for_modifiers = char_info["look_for_modifiers"]
            found_a_modifier = False
            # see if it has modifiers around it so we can count both chars as one
            if look_for_modifiers:
                modifiers_possible_before = char_info["modifiers_possible_before"]
                modifiers_possible_after = char_info["modifiers_possible_after"]
                # check for after modifiers
                if (j != num_chars - 1) and len(modifiers_possible_after) > 0:
                    if chars[j+1] in modifiers_possible_after:
                        emoji_found = char + chars[j+1]
                        # Debugging
                        print (char, [char])
                        print (chars[j+1], [chars[j+1]])
                        print (emoji_found)
                        print ("***************")
                        j += 1 # skip the next character because we found it
                        look_for_modifiers = False
                        found_a_modifier = True
                
                # check for before modifiers
                if (j != 0) and look_for_modifiers and len(modifiers_possible_before) > 0:
                    if chars[j-1] in modifiers_possible_before:
                        emoji_found = chars[j-1] + char
                        found_a_modifier = True
                        # Debugging
                        print (chars[j-1], [chars[j-1]])
                        print (char, [char])
                        print (emoji_found)
                        print ("***************")
                print(char)
                print(char_info['can_stand_alone'])
                # an extra check just to make sure it is an emoji
            if char_info["can_stand_alone"] or found_a_modifier:    
                reassembled.append(emoji_found)
        j += 1
        print(reassembled)
    return reassembled
def reassemble_emoji_from_tokens(tokens):
    return reassemble_emoji_from_utf8_char_list(tokens)


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
    parser.add_argument('-out_filename', type=str)
    parser.add_argument('-keep_emoji', action='store_true', help='keep emoji when removing symbols from tweets ')
    parser.add_argument('-single_retweets', action='store_true', help='Have only a signle copy of a retweeted status rather than as many times as it appears in the corpus.')
    parser.add_argument('-no_write', action='store_true', help='Don\'t write to an out file')
    parser.add_argument('-filter_for_en', action='store_true', help='Only select english tweets')

    opts = parser.parse_args()

    seen_retweets = set()
    batch_count = 0
    to_be_written = []
    for gz in sorted(glob.glob(opts.timelines_glob)):
        print(gz)
        file_dir = os.path.dirname(os.path.realpath('__file__'))
        out_filename = os.path.basename(gz)
        out_filename = out_filename[0:-3]+'_parsed_tweet_text.txt.gz'
        filename = os.path.join(file_dir, '../data/parsed_timelines', out_filename)
        filename = os.path.abspath(os.path.realpath(filename))

        f = gzip.open(filename,'wb')
        for tweet in tqdm(gzip_timeline_generator(gz)):
            if not tweet:
                continue

            tweet = tweet.decode("utf8").replace(",\n","")
            tweet = json.loads(tweet)

            # keep track of retweets and only have one copy of each
            # if we also have the original tweet in the corpus
            # that tweet will be represented twice
            if opts.single_retweets and 'retweeted_status' in tweet:
                retweeted_status = tweet['retweeted_status']
                if retweeted_status['id'] in seen_retweets:
                    continue
                else:
                    seen_retweets.add(retweeted_status['id'])
            if opts.filter_for_en:
                if tweet['lang'] != 'en':
                    continue

            text = tweet['text']

            """"
            form = 'NFKD'
            categories = set(['Mn', 'Sc', 'Sk', 'Sm', 'So'])

            for char in normalize(form, text):
                symbol_count_dict[char] += 1
                symbol_cat_dict[char] = category(char)
            continue
            """
            # process tweet for w2v
            # remove stop words, punctuation, urls, extra whitespace, accent marks, symbols
            text = cucco_normalizations(text,opts.keep_emoji)

            # tokenize
            tknzr = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=True)
            tokens = tknzr.tokenize(text)
            # normalize for spelling and abbreviations with Han and Baldwin 2012
            tokens = [emnlp_dict[t] if t in emnlp_dict else t for t in tokens]
            if not tokens:
                continue
            
            # get rid of the 'rt'
            if tokens[0] == 'rt':
                tokens = tokens[1:] 
            if opts.keep_emoji:
                # join flags and skintone
                new_tokens = []
                i = 0
                while i < len(tokens) - 1:
                    if FLAG_REGEX.search(tokens[i]) and FLAG_REGEX.search(tokens[i+1]):
                        new_tokens.append(''.join(tokens[i:i+2]))
                        # skip the next token
                        i += 2
                        """
                        # skin tones with zwj
                        elif tokens[i] == '‍' and tokens[i+1] in '🏻🏼🏽🏾🏿':
                            # replace the emoji that lacked skin tones
                            new_tokens[-1] = new_tokens[-1] + tokens[i] + tokens[i+1]
                            # skip the next token
                            i += 2
                        # skin tones without zwj
                        elif tokens[i] in '🏻🏼🏽🏾🏿' and i != 0:
                            # replace the emoji that lacked skin tones
                            new_tokens[-1] = new_tokens[-1] + tokens[i]
                            i += 1
                        """
                    # empty zwj
                    elif tokens[i] == '‍':
                        # skip it
                        i += 1
                    # ️ Variation Selector-16 ️ U+FE0F
                    # ⃣ Combining Enclosing Keycap
                    elif tokens[i]=='' or tokens[i] == '️⃣':
                        new_tokens[-1] = new_tokens[-1] + tokens[i]
                    # pride flag
                    elif tokens[i] == ' 🏳️‍' and tokens[i+1] == '🌈' :
                        new_tokens.append(''.join(tokens[i:i+7]))
                        # skip the next token
                        i += 2
                    # flags of GB states
                    elif tokens[i] == '🏴' and tokens[i+1] == '\U000E0067' :
                        new_tokens.append(''.join(tokens[i:i+7]))
                        # skip the five tokens
                        i += 7
                    else:
                        new_tokens.append(tokens[i])
                        i += 1

            tokens = new_tokens
            text = ' '.join(tokens)

            # remove new stop words
            text = remove_stop_words(text)
            to_be_written.append(text)
            batch_count += 1
            
            if batch_count > 10000:
                for tweet in to_be_written:
                    line = '%s\n' % tweet
                    f.write(line.encode('utf-8'))
                # reset batch
                batch_count = 0
                to_be_written = []
        # end of the gz doc
        # write what remains
        if batch_count:
            for tweet in to_be_written:
                line = '%s\n' % tweet
                f.write(line.encode('utf-8'))
            # reset batch
            batch_count = 0
            to_be_written = []