#!/bin/bash

#PBS -N LA_STREAM_16GiB
#PBS -q day
#PBS -l nodes=1:ppn=48:r662,walltime=24:00:00
#PBS -M a75655@alunos.uminho.pt

#set -x
node=662

module load intel/2013.1.117 gcc/6.1.0 boost/1.55.0 intel/openmpi_eth/1.8.2 papi/5.5.0
source /share/apps/intel/compilers_and_libraries_2019.0.117/linux/bin/compilervars.sh intel64

export LD_LIBRARY_PATH=/home/a75655/boost_1_69_0/lib:$LD_LIBRARY_PATH

cd /home/a75655/dbms_2
#cd /home/daniel/Documents/Tese/dbms-streaming-approach

#Query to test 8192 16384 32768 65536 131072 262144 524288 1048576 2097152
block_size=(32768 65536 131072)
sets=(32)

threads=(1 2 4 8 12 16 24 32 48)
work_threads=(1 2 3 4 5 6 7 8 9)
read_threads=(2 2 2 2 3 3 3 3 3)
dot_threads=(1 2 3 4 5 6 7 8 9)
had_threads=(2 2 2 2 3 3 3 3 3)

q="6_reuse_PAPI"

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

        make delete
        make clean_stream
        make
        make load

        ii=0
        #Threads ...
        for t in "${threads[@]}"
        do
            export OMP_NUM_THREADS="${t}"
            export WORK_THREADS="${work_threads[${ii}]}"
            export READ_THREADS="${read_threads[${ii}]}"
            export DOT_THREADS="${dot_threads[${ii}]}"
            export HAD_THREADS="${had_threads[${ii}]}"

            echo "BUFFER SIZE ........................................................"
            echo $WORK_THREADS
            echo $READ_THREADS
            echo $DOT_THREADS
            echo $HAD_THREADS

            for q in "6_reuse_time_load" "6_s_reuse_time_load"
            do
                make clean
                make
                cp -f stream/bin/"q${q}" engine/bin
                ./engine/bin/"q${q}"

                time_file="perf_collect/results/query_${q}_time.csv"

                if [ "${q}" = "6_s_reuse_time_load" ]; then
                    echo -n "${work_threads[${ii}]},${read_threads[${ii}]},${dot_threads[${ii}]},${had_threads[${ii}]}," >> "${time_file}"
                else
                    echo -n "${t}," >> "${time_file}"
                fi

                 # Test the current dataset
                /opt/python/bin/python2.7 "perf_collect/time_load.py" "${i}" "${blSize}" "${q}" "query_${q}"
            done
            ((ii++))
        done
    done
done

make delete
cd data/dbgen/
echo "Create dataSet $1"
rm -r -f 1/ 2/ 4/ 8/ 16/ 32/ 64/