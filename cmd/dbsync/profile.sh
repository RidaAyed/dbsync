#!/bin/bash
for i in `seq 1 10`;
do
   echo "Create heap profile $i.heap"
   curl -s http://localhost:8080/debug/pprof/heap > $i.heap
   sleep 5;
done

echo "DONE"
