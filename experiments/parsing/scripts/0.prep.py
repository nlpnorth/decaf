from nltk.tokenize import sent_tokenize
import myutils
import nltk
nltk.download('punkt_tab')

for dataset in myutils.datasets:
    out_file = open(myutils.data_folder + dataset +  '.sents', 'w')
    for line in open(myutils.data_folder + dataset):
        for line in sent_tokenize(line):
            out_file.write(line + '\n')
    out_file.close()

