#!/bin/bash
#PBS -l nodes=1:ppn=2,walltime=30:00:00
#PBS -M vmwong@indiana.edu
#PBS -m abe
#PBS -N generating_binnednets

module switch python/2.7.3 python/3.6.3

echo "########### begin thing.py ###########"
start=`date +%s`
python3 analyzing_networks.py
end=`date +%s`
runtime=$((end-start))
echo "########### end thing.py ###########"
echo $runtime
