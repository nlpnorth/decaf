import os

for dataset in os.listdir('train_100M'):
    if not dataset.endswith('.train'):
        continue
    print(dataset)
    
    datasplits = ['train_100M/' + x for x in os.listdir('train_100M') if x.startswith(dataset) and 'conllu' in x]
    sorted_list = sorted(datasplits)
    
    final_out = open('train_100M/' + dataset + '.complete', 'w')
    for pred_file in sorted_list:
        txt_file = pred_file.replace('.conllu', '')
        # TODO merge txt into conll-format
        txt_data = open(txt_file).readlines()
        sent_idx = 0
        curSent = []
        for line in open(pred_file):
            if len(line) < 3:
                while len(txt_data[sent_idx].strip()) == 0:
                    sent_idx += 1
                final_out.write('# text = ' + txt_data[sent_idx])
                for tokked_line in curSent:
                    final_out.write('\t'.join(tokked_line) + '\n')
                final_out.write('\n')
                sent_idx += 1
                curSent = []
            else:
                curSent.append(line.strip().split('\t'))
        #break
    final_out.close()
    #break

