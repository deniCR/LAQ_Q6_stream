#!/bin/bash

#PBS -N Vtune_collection_781
#PBS -q day
#PBS -l nodes=1:ppn=48:r662,walltime=1:00:00
#PBS -M a75655@alunos.uminho.pt

module load intel/2013.1.117 gcc/6.1.0 boost/1.55.0 intel/openmpi_eth/1.8.2 papi/5.5.0
source /share/apps/intel/compilers_and_libraries_2019.0.117/linux/bin/compilervars.sh intel64
source /share/apps/intel/vtune_amplifier/sep_vars.sh && source /share/apps/intel/vtune_amplifier/amplxe-vars.sh

export LD_LIBRARY_PATH=/home/a75655/boost_1_69_0/lib:$LD_LIBRARY_PATH

cd /home/a75655/dbms_2

node=662

exec_dir="/home/a75655/dbms_2"
result_dir="/home/a75655/dbms_2/flame_graph/${node}/results/"

result_dir_th="/home/a75655/dbms_2/Vtune_collect/${node}/results_th_vecS/"
result_dir_hs="/home/a75655/dbms_2/Vtune_collect/${node}/results_hs_vecS/"

mkdir -p "${result_dir_th}"
mkdir -p "${result_dir_hs}"

log_file="/home/a75655/dbms_2/Vtune_collect/collection.log"

queries=("6_HEPFrame_remove" "6_HEPFrame_remove_all" "6_HEPFrame_reuse" "6_HEPFrame_reuse_all")

#Query to Test  1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152
block_size=(32768 65536)
block_name=("32Ki" "64Ki")

sets=(1)
threads_per_event=(1 2 4 8)

work_threads=(8 12 18 24)
read_threads=(2 2 2 4 4)
dot_threads=(4 8 8 12 12)
had_threads=(2 3 3 3 3)

q=("6_HEPFrame")

for i in "${sets[@]}"
do
    #./set_tcph.sh "${i}"
    export DATASET=${i}
    echo $DATASET
    
    #Query to test 32768 65536 131072 262144 524288 1048576 2097152
    for blSize in "${block_size[@]}"
    do
        export BSIZE="${blSize}"

        #make delete
        #make clean
        #make
        #make load

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

        rm -r /home/a75655/dbms_2/r000runss

        ii=0
        #Threads ...
        for t in "${threads_per_event[@]}"
        do
            export THREADS_PER_EVENT="${t}"

            #make hepf

            for q in "${queries[@]}"
            do
                cp -f hepf/bin/"${q}" engine/bin
                ./engine/bin/"${q}"
                 # Test the current dataset
                result_dir_th_q="${result_dir_th}${q}_${i}_${bb_name}_${t}"
                result_dir_hs_q="${result_dir_hs}${q}_${i}_${bb_name}_${t}"

                amplxe-cl -collect-with runss -duration=300 -knob cpu-samples-mode=stack -knob waits-mode=stack -knob signals-mode=stack -knob io-mode=stack -knob enable-user-tasks=true -knob enable-user-sync=true -knob event-config=INST_RETIRED.ANY:sa=2000003,MEM_LOAD_UOPS_LLC_MISS_RETIRED.LOCAL_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT:sa=2000003,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L1_MISS:sa=100003,MEM_LOAD_UOPS_RETIRED.L1_MISS_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.L2_HIT:sa=100003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.L2_MISS:sa=50021,MEM_LOAD_UOPS_RETIRED.L2_MISS_PS:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_HIT_PS:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_LOAD_UOPS_RETIRED.LLC_MISS_PS:sa=100007 -knob mrte-type=java,python -knob analyze-openmp=true -target-duration-type=veryshort -allow-multiple-runs -finalization-mode=full --result-dir "${result_dir_th_q}" -app-working-dir /home/a75655/dbms_2 -- "/home/a75655/dbms_2/engine/bin/${q}"
                mv "r000runss" "${result_dir_th_q}"
                amplxe-cl -collect-with runss -duration=300 -knob cpu-samples-mode=stack -knob waits-mode=stack -knob signals-mode=stack -knob enable-user-tasks=true -knob enable-user-sync=true -knob mrte-type=java,python -knob analyze-openmp=true -target-duration-type=veryshort -allow-multiple-runs -finalization-mode=full --result-dir "${result_dir_hs_q}" -app-working-dir /home/a75655/dbms_2 -- "/home/a75655/dbms_2/engine/bin/${q}"
                mv "r000runss" "${result_dir_hs_q}"

            done
            ((ii++))
        done
    done
done

make delete
cd data/dbgen/
echo "Create dataSet $1"
rm -r -f 1/ 2/ 4/ 8/ 16/ 32/ 64/
