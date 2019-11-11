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

  print num

  for num in queries_keys:
    query = "q" + num

    exec_time = []

    result = []

    h=20;w=20;
    times = [[] for y in range(h)] 

    for i in range(15):

      print QUERIES_DIR
      print query

      # run query
      execution = subprocess.Popen(
        [
          "./{0}/{1}".format(QUERIES_DIR,query)
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE
      )

      # get query results
      output, _ = execution.communicate(input=query.format(data_size))
      output_line_list = output.splitlines()

      print output

      # calculate execution timeexecution
      print output_line_list

      t_acc = float(output_line_list[1])
      exec_time.append( t_acc )

      for ix in range(2,20):
        times[ix-2].append( float(output_line_list[ix]))

      print (t_acc)

      print times

      print OUTPUT_FILE

    # aggregate data from multiple runs storing the k-best, average and standard deviation
    with open("perf_collect/results/{0}_time.csv".format(OUTPUT_FILE), "a") as csv_time:
      kb = round( kbest(5,exec_time), 6)
      av = round( avg(exec_time), 6)
      sd = round( sdev(av,exec_time), 6)
      csv_time.write( ",{0},{1},{2}".format(kb, av, sd))
      for ix in range(0,18):
        csv_time.write( ",{0}".format(avg(times[ix])))
      csv_time.write("\n");

if __name__ == "__main__":
  main(sys.argv)
