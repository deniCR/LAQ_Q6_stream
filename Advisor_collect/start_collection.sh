#!/bin/bash

#PBS -N Vtune_collection
#PBS -q day
#PBS -l nodes=compute-662-5,walltime=12:00:00
#PBS -M a75655@alunos.uminho.pt

set -x
module load intel/2013.1.117 gcc/6.1.0 boost/1.55.0 intel/openmpi_eth/1.8.2
source /share/apps/intel/compilers_and_libraries_2019.0.117/linux/bin/compilervars.sh intel64
source /media/daniel/LINUX/Program/system_studio_2019/advisor/advixe-vars.sh

export LD_LIBRARY_PATH=/home/a75655/boost_1_69_0/lib:$LD_LIBRARY_PATH

cd /home/a75655/dbms_2

node=662

result_dir_th="/home/a75655/dbms_2/Vtune_collect/${node}/results_th_vecS/"
result_dir_hs="/home/a75655/dbms_2/Vtune_collect/${node}/results_hs_vecS/"

mkdir -p "${result_dir_th}"
mkdir -p "${result_dir_hs}"

log_file="/home/a75655/dbms_2/Vtune_collect/collection.log"

#Query to Test  1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152
block_size=(262144 524288 1048576)
block_name=("256Ki" "512Ki" "1Mi")
sets=(32)
buffer_size=(1)
threads=(10 20 40)
work_threads=(8 12 16)
read_threads=(2 2 2 2 2)
dot_threads=(4 8 8 8 8)
had_threads=(2 3 3 3 3)

for i in "${sets[@]}"
do
    ./set_tcph.sh "${i}"
    export DATASET=${i}
    echo $DATASET
    #Query to Test  32768 65536 131072 262144 524288 1048576 2097152
    bb=0
    for blSize in "${block_size[@]}"
    do
        export BSIZE="${blSize}"
        bb_name="${block_name[${bb}]}"
        make delete
        make
        make load

        for q in "6_s"
        do
            echo "" >> "${log_file}"
            echo "Query: ${q}"
            echo "#########################################" >> "${log_file}"
            echo "TIME load" >> "${log_file}"
            date >> "${log_file}"
            echo "Node 662,1 socket test other data set" >> "${log_file}"
            echo "Sets: ${sets[@]}" >> "${log_file}"
            echo "Block_size: ${blSize}" >> "${log_file}"
            echo "Threads: ${threads[@]}" >> "${log_file}"
            echo "#########################################" >> "${log_file}"
            #Threads ...

            ii=0

            rm -r /home/a75655/dbms_2/r000runss

            if [ "${q}" = "6" ]
            then 

                #Threads ...
                for t in "${threads[@]}"
                do
                    export OMP_NUM_THREADS=48
                    make "q${q}"
                    export OMP_NUM_THREADS="${t}"
                    # Test the current dataset
                    result_dir_th_q="${result_dir_th}${q}_${i}_${bb_name}_${t}"
                    result_dir_hs_q="${result_dir_hs}${q}_${i}_${bb_name}_${t}"

                    advixe-cl -collect survey -mrte-mode=managed -interval=2 -data-limit=1000 -profile-python -support-multi-isa-binaries -spill-analysis -static-instruction-mix -project-dir /home/a75655/intel/advixe/projects/DBMS --search-dir src:rp=/home/a75655/dbms_2/engine --search-dir src:rp=/home/a75655/dbms_2/queries/cpp --search-dir src:rp=/home/a75655/dbms_2/engine/src --search-dir src:rp=/home/a75655/dbms_2/engine/include -- /home/a75655/dbms_2/engine/bin/q6
                    advixe-cl -collect tripcounts -flop -stacks -profile-python -project-dir /home/a75655/intel/advixe/projects/DBMS --search-dir src:rp=/home/a75655/dbms_2/engine --search-dir src:rp=/home/a75655/dbms_2/queries/cpp --search-dir src:rp=/home/a75655/dbms_2/engine/include --search-dir src:rp=/home/a75655/dbms_2/engine/src -- /home/a75655/dbms_2/engine/bin/q6
                done
            else
                #Threads ...
                for t in "${work_threads[@]}"
                do
                    export WORK_THREADS="${t}"
                    export READ_THREADS="${read_threads[${ii}]}"
                    export DOT_THREADS="${dot_threads[${ii}]}"
                    export HAD_THREADS="${had_threads[${ii}]}"

                    echo $WORK_THREADS "${log_file}"
                    echo $READ_THREADS "${log_file}"
                    echo $DOT_THREADS "${log_file}"
                    echo $HAD_THREADS "${log_file}"

                    for bb in "${buffer_size[@]}"
                    do
                        rm stream/bin/"q${q}"
                        rm engine/bin/"q${q}"

                        make "q${q}"
                        cp stream/bin/"q${q}" engine/bin/

                        result_dir_th_q="${result_dir_th}${q}_${i}_${bb_name}_${t}_${bb}"
                        result_dir_hs_q="${result_dir_hs}${q}_${i}_${bb_name}_${t}_${bb}"

                        # Test the current dataset
                    advixe-cl -collect survey -mrte-mode=managed -interval=2 -data-limit=1000 -profile-python -support-multi-isa-binaries -spill-analysis -static-instruction-mix -project-dir /home/a75655/intel/advixe/projects/DBMS_Stream --search-dir src:rp=/home/a75655/dbms_2/engine --search-dir src:rp=/home/a75655/dbms_2/stream --search-dir src:rp=/home/a75655/dbms_2/Channel/include --search-dir src:rp=/home/a75655/dbms_2/engine/src -- /home/a75655/dbms_2/stream/bin/q6_s
                    advixe-cl -collect tripcounts -flop -stacks -profile-python -project-dir /home/a75655/intel/advixe/projects/DBMS_Stream --search-dir src:rp=/home/a75655/dbms_2/engine --search-dir src:rp=/home/a75655/dbms_2/stream --search-dir src:rp=/home/a75655/dbms_2/Channel/include --search-dir src:rp=/home/a75655/dbms_2/queries/cpp -- /home/a75655/dbms_2/stream/bin/q6_s
                    done
                    ((ii++))
                done
            fi
        done
        ((bb++))
    done
done

#make deleteAll