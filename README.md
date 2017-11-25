# file_deduper

script will find duplicate files in 3 passes. First builds a list of files by size and second compares the first 10K of the files hash to see if they are the same and last compare all the files with the same first 10K hash to see if the files are equal.
