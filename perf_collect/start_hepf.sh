#!/bin/bash

#PBS -N LA_HEPF
#PBS -q day
#PBS -l nodes=1:ppn=46:r662,walltime=24:00:00
#PBS -l mem=56GB
#PBS -M a75655@alunos.uminho.pt

#set -x
node=652

module load intel/2013.1.117 gcc/6.1.0 boost/1.55.0 intel/openmpi_eth/1.8.2 papi/5.5.0
source /share/apps/intel/compilers_and_libraries_2019.0.117/linux/bin/compilervars.sh intel64

export LD_LIBRARY_PATH=/home/a75655/boost_1_69_0/lib:$LD_LIBRARY_PATH

cd /home/a75655/dbms_2

queries=("6_HEPFrame_remove" "6_HEPFrame_remove_all" "6_HEPFrame_reuse" "6_HEPFrame_reuse_all")

#Query to test 8192 16384 32768 65536 131072 262144 524288 1048576 2097152
block_size=(32768 65536 131072)
sets=(32)

threads=(1 2 4)

q="6_HEPFrame_remove"

#START MEMORY MONITOR
/opt/python/bin/python2.7 "memory_monitor.py" &
monitor_pid=($!)

#Data sets 1 ...
for i in "${sets[@]}"
do
    ./set_tcph.sh "${i}"
    export DATASET=${i}
    echo $DATASET
    
    #Query to test 32768 65536 131072 262144 524288 1048576 2097152
    for blSize in "${block_size[@]}"
    do
        export BSIZE="${blSize}"

        make delete
        make clean
        make
        make load

        time_file="perf_collect/results/query_${q}_time.csv"
        memory_file="perf_collect/results/query_${q}_memory.csv"
        echo "" | \
            tee -a "${time_file}" "${memory_file}"
        echo "#########################################" | \
            tee -a "${time_file}" "${memory_file}"
        date | tee -a "${time_file}" "${memory_file}"
        echo "Lock Version ..." | tee -a "${time_file}" "${memory_file}"
        echo "Node ${node},1 socket test other data set" | \
            tee -a "${time_file}" "${memory_file}"
        echo "Work_threads: ${work_threads[@]} Buffer_size:" | \
            tee -a "${time_file}" "${memory_file}"
        echo $BUFFER_SIZE | \
            tee -a "${time_file}" "${memory_file}"
        echo "#########################################" | \
            tee -a "${time_file}" "${memory_file}"
        echo "block_size,dataset,k-best,average,std_dev" | \
            tee -a "${time_file}" "${memory_file}"

        ii=0
        #Threads ...
        for t in "${threads[@]}"
        do
            export THREADS_PER_EVENT="${t}"

            make hepf

            for q in "${queries[@]}"
            do
                cp -f hepf/bin/"${q}" engine/bin
                ./engine/bin/"${q}"
                 # Test the current dataset
                /opt/python/bin/python2.7 "perf_collect/perf.py" "${i}" "${blSize}" "${q}" "query_${q}"
            done
            ((ii++))
        done
    done
done

kill -2 "${monitor_pid}"

make delete
cd data/dbgen/
echo "Create dataSet $1"
rm -r -f 1/ 2/ 4/ 8/ 16/ 32/ 64/