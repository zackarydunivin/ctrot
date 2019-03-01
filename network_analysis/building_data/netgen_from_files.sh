#!/bin/bash
#PBS -l nodes=2:ppn=4,walltime=15:00:00
#PBS -M vmwong@indiana.edu
#PBS -m abe
#PBS -N generating_networks_from_files

module switch python/2.7.3 python/3.6.3

echo "########### begin thing.py ###########"
start=`date +%s`
python3 netgen_from_files.py 7
end=`date +%s`
runtime=$((end-start))
echo "########### end thing.py ###########"
echo $runtime