
# download babylm data from https://osf.io/rduj2
mv ~/Downloads/train_100M.zip .
unzip train_100M.zip


# Install MaChAmp
git clone https://github.com/machamp-nlp/machamp.git
cd machamp
pip3 install -r requirements

# get UD data
mkdir data
cd data
wget https://github.com/UniversalDependencies/UD_English-GUM/archive/refs/tags/r2.15.zip
unzip r2.15.zip
cd ../
python3 scripts/misc/cleanconl.py data/UD_English-GUM-r2.15/*conllu

cd ../
python3 scripts/0.prep.py


