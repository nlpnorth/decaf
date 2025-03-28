import _jsonnet
import json
import os
import myutils

def load_json(path: str):
    return json.loads(_jsonnet.evaluate_snippet("", '\n'.join(open(path).readlines())))

def makeParams(defaultPath, mlm):
    config = load_json(defaultPath)
    config['transformer_model'] = mlm
    tgt_path = 'configs/params.' + mlm.split('/')[-1] + '.json'
    if not os.path.isfile(tgt_path):
        json.dump(config, open(tgt_path, 'w'), indent=4)
    return tgt_path





datasets = []
for ud_folder in ['machamp/data/ud-unofficial/',  'machamp/data/ud-treebanks-v2.15/']:
    for dataset in os.listdir(ud_folder):
        if not dataset.startswith('UD_English'):
            continue
        if "ConvBank" in dataset or "CHILDES" in dataset:
            continue
        train, dev, test = myutils.getTrainDevTest(ud_folder + dataset + '/')
        if train != '':
            config = load_json('configs/gum.json')
            config['UD_EWT']['train_data_path'] = train.replace('machamp/', '')
            if dev != '':
                config['UD_EWT']['dev_data_path'] = dev.replace('machamp/', '')
            else:
                config['UD_EWT']['dev_data_path'] = test.replace('machamp/', '')
            
            json.dump(config, open('configs/' + dataset + '.json', 'w'), indent=4)
            datasets.append(dataset)

for lm in myutils.lms: 
    # create config
    lm_config_path = makeParams('machamp/configs/params.json', lm)
    all_sets = ''
    for dataset in datasets:
        # train
        name = dataset + '.' + lm.split('/')[-1]
        cmd = 'python3 train.py --parameters_config ../' + lm_config_path.replace('machamp/', '') + ' --dataset_config ../configs/' + dataset + '.json --name ' + name
        all_sets.append('../configs/'+ dataset  + '.json')
        print(cmd)

    name = lm.split('/')[-1] + '-all'
    cmd = 'python3 train.py --parameters_config ../' + lm_config_path.replace('machamp/', '') + ' ' + ' '.join(all_sets) + ' --name ' + name
    all_sets.append('../configs/'+ dataset  + '.json')
    print(cmd)
    
