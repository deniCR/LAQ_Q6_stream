#!/usr/bin/env python

import sys, subprocess, os, time, re
from datetime import datetime

def kbest (k, values):
  error=(1,-1)
  values.sort()
  for i in range(len(values)-k):
    maximum = values[i+k-1]
    minimum = values[i]
    if maximum==0:
      e=0
    else:
      e = (maximum - minimum) / float(maximum)
    if e < 0.15:
      return sum(values[i:i+k]) / float(k)
    if e < error[0]:
      error=(e,i)
  if error[1] != -1:
    return sum(values[error[1]:error[1]+k]) / float(k)
  return -1

def avg(values):
  if (len(values)>0):
    return sum(values) / float(len(values))
  return 0

def sdev(mean, values):
  return avg(map(lambda x: (x-mean)**2, values))**(0.5)

def main(argv):

  data_size = argv[1]
  block_size = argv[2]
  QUERIES_STRING = argv[3]
  OUTPUT_FILE = argv[4]

  QUERIES_DIR="engine/bin"
  DSTAT_DIR="lib/dstat"

  queries_keys = QUERIES_STRING.split(",")

  for num in queries_keys:
    with open("perf_collect/results/{0}_time.csv".format(OUTPUT_FILE), "a") as csv_time:
      csv_time.write("{0},{1}".format(block_size,data_size))
    with open("perf_collect/results/{0}_memory.csv".format(OUTPUT_FILE), "a") as csv_memory:
      csv_memory.write("{0},{1}".format(block_size,data_size))

  print num

  for num in queries_keys:
    query = "" + num

    exec_time = []
    tot_cyc_a = []
    tot_ins_a = []
    BR_INS_a = []
    SR_INS_a = []
    LD_INS_a = []
    CPI_a = []
    CPE_a = []
    Bandwidth_a = []

    result = []

    h=24;w=20;
    exec_perf = [[] for y in range(h)] 
    memory = [[] for y in range(h)]

    for i in range(25):

      if os.path.isfile("la_memory_usage.csv"):
          os.remove("la_memory_usage.csv")

      dstat = subprocess.Popen(
        [DSTAT_DIR+"/dstatB",
        "--cpu-use",
        "--noheaders",
        "--noupdate",
        "--nocolor",
        "--output",
        "la_memory_usage.csv"],
        stdout=subprocess.PIPE
      )
      time.sleep(2)

      print QUERIES_DIR
      print query

      # run query
      execution = subprocess.Popen(
        [
          "perf","stat",
          "-ecycles",
          "-einstructions",
          "-ecache-references",
          "-ecache-misses,branches",
          "-ebranch-misses",
          "-ebus-cycles",
          "-efaults",
          "./{0}/{1}".format(QUERIES_DIR,query),
          "2>",
          "null"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE
      )

      for w in range(480):
        time.sleep(1)
        r = execution.poll()
        if r != None :
         break

      if r != None:

        # get query results
        output, _ = execution.communicate(input=query.format(data_size))
        output_line_list = output.splitlines()

        # stop profiler
        time.sleep(1)
        dstat.terminate()

        # get profiler results
        mem_out_list=[]
        with open("la_memory_usage.csv", "r") as mem_info:
          mem_out_list = mem_info.readlines()[7:]
        os.remove("la_memory_usage.csv")

        #print mem_out_list

        # calculate max memory usage
        for m in range(h):
          memory[m].append(
            max(map( lambda x: float(x.split(',')[m]), mem_out_list ))
          )

        #print memory

        # calculate execution timeexecution
        print output_line_list

        if len(output_line_list) > 0:
          if output_line_list[0]!="Segmentation fault (core dumped)":

            try:
              t_acc = float(output_line_list[1])
              exec_time.append( t_acc )

              for ix in range(2,11):
                print output_line_list[5+ix]
                g = (re.search('[0-9]+', output_line_list[3+ix]))
                if g:
                  exec_perf[ix-2].append( float(g.group(0) ))
            except ValueError:
              print "Fail"
      else:
        execution.kill()

    # aggregate data from multiple runs storing the k-best, average and standard deviation
    with open("perf_collect/results/{0}_time.csv".format(OUTPUT_FILE), "a") as csv_time:
      kb = round( kbest(5,exec_time), 6)
      av = round( avg(exec_time), 6)
      sd = round( sdev(av,exec_time), 6)
      csv_time.write( ",{0},{1},{2}".format(kb, av, sd))
      
      for ix in range(0,9):
        csv_time.write( ",{0}".format(avg(exec_perf[ix])))
      csv_time.write("\n");
    with open("perf_collect/results/{0}_memory.csv".format(OUTPUT_FILE), "a") as csv_memory:

      #kb = round( kbest(5,memory), 6)
      #av = round( avg(memory), 6)
      #sd = round( sdev(av,memory), 6)
      #csv_memory.write( ",{0},{1},{2}\n".format(kb, av, sd) )
      for m in range(h):
        csv_memory.write( "{0},".format(avg(memory[m])))
      csv_memory.write("\n")

if __name__ == "__main__":
  main(sys.argv)
