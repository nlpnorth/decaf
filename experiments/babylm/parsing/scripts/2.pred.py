import conll18_ud_eval
import pprint
import os
import myutils
import json

def getModel(name):
    modelDir = 'machamp/logs/'
    nameDir = modelDir + name + '/'
    if os.path.isdir(nameDir):
        for modelDir in reversed(os.listdir(nameDir)):
            modelPath = nameDir + modelDir + '/model.pt'
            if os.path.isfile(modelPath):
                return modelPath
    return ''

datasets = [dataset.replace('.json', '') for dataset in os.listdir('configs') if dataset.startswith('UD')]

for lm in myutils.lms:
    for src_dataset in datasets:
        name = src_dataset + '.' + lm.split('/')[-1]
        model_path = getModel(name)
        if model_path != '':
            for tgt_dataset  in datasets:
                
                out_path = '../preds/' + name + '-' + tgt_dataset
                dev_path = json.load(open('configs/' + tgt_dataset + '.json'))['UD_EWT']['dev_data_path']
                cmd = 'python3 predict.py ' + model_path.replace('machamp/', '') + ' ' + dev_path + ' ' + out_path
                print(cmd)
