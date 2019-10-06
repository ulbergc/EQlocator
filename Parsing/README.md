# Parsing
Insight Data Engineering project, Seattle 2019C session  
Carl Ulberg  

Use these codes to parse earthquake data from original format and augment the dataset to test throughput of the Processing/ methods

1. First run parse_to_json.py with `python parse_to_json.py outdir`, where `outdir` is the directory to write the new .json files to.
2. Then run augment_json.py with `./run_augment.sh`
  
   You can modify `ncopy`, `mn0`, `mn1` values within run_augment.sh to make an arbitrarily large dataset
