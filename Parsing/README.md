# Parsing
Insight Data Engineering project, Seattle 2019C session  
Carl Ulberg  

Use these codes to parse earthquake data from original format and augment the dataset to test throughput of the Processing/ methods

1. First run parse_to_json.py with `python parse_to_json.py outdir`, where `outdir` is the directory to write the new .json files to.
2. Then run augment_json.py with `./run_augment.sh`
  
   You can modify `ncopy`, `mn0`, `mn1` values within run_augment.sh to make an arbitrarily large dataset

3. Move files to AWS S3 bucket by navigating to the directory containing them ($bigdir/big in run_augment.sh) and calling:

   `aws s3 sync . s3://<bucket_name>/<path_to_files_in_bucket>`
   
   This will require the AWS command line interface [tool](https://aws.amazon.com/cli/)
