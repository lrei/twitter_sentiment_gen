import langid
import sys

def filter_langid(text, lang=u'en', langid_min_prob=0.80):
    #
    # Filter based on language using langid
    #
    print(langid_min_prob)
    if langid_min_prob >= 0 and text is not None:
        # Check if identified language is the expected language
        lid, prob = langid.classify(text)
        if lid != lang:
            return None

        # Filter based ong langid minimum probability
        if prob < langid_min_prob:
            return None

    return text
    
def main():
    lang_code = u'en'
    langid_min_prob = 0.8
    if len(sys.argv) not in [2, 3, 4]:
        print(len(sys.argv))
        print(sys.argv[0] + ' tweet_text [lang_code] [langid_min_prob]')
        sys.exit(0)
    
    text = sys.argv[1]
    if len(sys.argv) > 2:
        lang_code = sys.argv[2]
    
    if len(sys.argv) > 3:
        langid_min_prob = sys.argv[3]
    
    filtered_text = filter_langid(text, lang_code, langid_min_prob)
    
    print(filtered_text)
   
   
if __name__ == '__main__':
    main()