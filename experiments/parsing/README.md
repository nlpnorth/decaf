# Obtaining predictions of morphosyntax for DECAF

This folder contains the scripts that are needed to reproduce our annotations of the 
BabyLM corpus. In the scripts folder, you can find the `runall.sh` script which contains
the exact commands needed to reproduce the corpus. 

It should be noted that the scripts are best parallelized and ran on GPU's, as it is 
training transformer-based language models, and running inference on large amounts of data.
