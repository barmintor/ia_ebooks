# ia_ebooks.py

A python script that will download Internet Archive ebook collection records, munge them up with CLIO MARC data and add some convenient links.

## Prerequisites
This is a python 3 script that depends on requests and pymarc, see also requirements.txt

## Usage
print help:
```bash
python3 ia_ebooks.py -h
```

fetch a collection, as json, with CLIO data:
```bash
python3 ia_ebooks.py list-ebooks -C muslim-world-manuscripts -F json --clio > mwm.json
```

Command line results are printed to stdout!

Adding CLIO data subjects the script to rate limiting; messages will be printed to stderr if applicable.