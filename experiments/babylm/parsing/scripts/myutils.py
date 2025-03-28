datasets = ['bnc_spoken.train', 'childes.train', 'gutenberg.train', 'open_subtitles.train', 'simple_wiki.train', 'switchboard.train']

data_folder = 'train_100M/'

#lms = ['answerdotai/ModernBERT-large', 'xlm-roberta-large', 'cis-lmu/glot500-base', 'studio-ousia/mluke-large', 'studio-ousia/luke-large', 'microsoft/deberta-v3-large']
lms = ['xlm-roberta-large', 'studio-ousia/mluke-large', 'studio-ousia/luke-large', 'microsoft/deberta-v3-large', 'Twitter/twhin-bert-large']

def getTrainDevTest(path):
    train = ''
    dev = ''
    test = ''
    for conlFile in os.listdir(path):
        if conlFile.endswith('conllu'):
            if 'train' in conlFile:
                train = path + '/' + conlFile
            if 'dev' in conlFile:
                dev = path + '/' + conlFile
            if 'test' in conlFile:
                test = path + '/' + conlFile
    return train, dev, test

