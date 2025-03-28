import conll18_ud_eval
import pprint
import os
import myutils


name = 'UD_English-GUM.deberta-v3-large'

def getModel(name):
    modelDir = 'machamp/logs/'
    nameDir = modelDir + name + '/'
    if os.path.isdir(nameDir):
        for modelDir in reversed(os.listdir(nameDir)):
            modelPath = nameDir + modelDir + '/model.pt'
            if os.path.isfile(modelPath):
                return modelPath
    return ''

for dataset in myutils.datasets:
    path = 'train_100M/' + dataset + '.sents'
    model_path = getModel(name)
    split_cmd = 'split ' + path + ' -l 250000 ' + path + '.'
    os.system(split_cmd)
    for i in range(26):
        split = 'a' + chr(ord('a') + i)
        split_file = '../' + path + '.' + split
        if os.path.isfile(split_file[3:]):
            cmd = 'python3 predict.py ' + model_path.replace('machamp/', '') + ' ' + split_file + ' ' + split_file + '.conllu --raw_text' 
            print(cmd)

