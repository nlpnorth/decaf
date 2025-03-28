./scripts/0.prep.sh

# train models
cd ../
python3 scripts/1.train.py > machamp/1.train.sh
cd machamp
chmod +x 1.train.sh
./1.train.sh
cd ../

# evaluate models
python3 scripts/2.pred.py > machamp/2.pred.sh
cd machamp
chmod +x 2.pred.sh
./2.pred.sh
cd ../

python3 2.eval.py

# now predict on babyLM
python3 scripts/3.pred.py > machamp/3.pred.sh
chmod +x 3.pred.sh
./3.pred.sh
cd ../

python3 scripts/4.merge.py
