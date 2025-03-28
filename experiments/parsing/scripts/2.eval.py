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

def pprint(scores):
    total = 0.0
    for metric in ['LAS', 'UPOS', 'XPOS', 'Lemmas', 'UFeats']:
        print(metric + ':\t{:.2f}'.format(scores[metric].f1*100))
        total += scores[metric].f1*100
    print('Avg:\t{:.2f}'.format(total/5))
     
def eval_s_type(gold_path, pred_path):
    gold_labels = [x.strip().split(' ')[-1] for x in open(gold_path) if x.startswith('# s_type')]
    pred_labels = [x.strip().split(' ')[-1] for x in open(pred_path) if x.startswith('# s_type')]
    cor = sum([int(gold ==pred) for gold, pred in zip(gold_labels, pred_labels)])
    return 100* cor/len(gold_labels)

datasets = [dataset.replace('.json', '') for dataset in os.listdir('configs') if dataset.startswith('UD')]


finals = {}
for lm in myutils.lms:
    for src_dataset in datasets:
        name = src_dataset + '.' + lm.split('/')[-1]
        total_scores = []
        for tgt_dataset in datasets:
            pred_path = 'preds/' + name + '-' + tgt_dataset
            gold_path = 'machamp/' + json.load(open('configs/' + tgt_dataset + '.json'))['UD_EWT']['dev_data_path']
            try:
                if not os.path.isfile(pred_path) or len(open(pred_path).readlines()) < 3 or not os.path.isfile(gold_path):
                    continue
                print(gold_path, pred_path)
                gold_file = conll18_ud_eval.load_conllu_file(gold_path)
                pred_file = conll18_ud_eval.load_conllu_file(pred_path)
                pprint(conll18_ud_eval.evaluate(gold_file, pred_file))
            except:
                print('ERR', pred_path)
                continue
            #s_type_score = eval_s_type('machamp/data/UD_English-GUM-r2.15/en_gum-ud-dev.conllu', pred_path)
            #print('s_type:\t{:.2f}'.format(s_type_score))
            avg_score = 0.0 # TODO
            total_score.append(avg_score)
        if len(total_scores) > 0:
            finals[name] = sum(total_scores)/len(total_scores)


for name in finals:
    print(name, '{:.2f}'.format(finals[name]))
