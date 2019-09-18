#Most code taken from lecture slides.
from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

def main():
    '''
    Part 1 code starts here
    '''
    sc1 = SparkContext()
    lines = sc1.textFile(sys.argv[1])
    count = lines.map(lambda x: x.split(",")).\
            filter(lambda x: len(x) == 17).\
            map(lambda x: (x[0],1)).\
            reduceByKey(add)
    temp_output = count.collect()
    output = sorted(temp_output, key=lambda tup: tup[1])[-10:]
    output = list(reversed(output))
    fh = open(sys.argv[2],"w") 
    for i in range(len(output)):
        fh.write(str(output[i]))
        fh.write("\n")
    fh.close()
    sc1.stop()
    '''
    Part 2 code starts here
    '''
    sc2 = SparkContext()
    sc2.stop()
    '''
    Part 3 code starts here
    '''
    sc3 = SparkContext()
    sc3.stop()
    
if __name__ == "__main__":
    '''
    Just chekcing if the arguements given are proper.
    '''
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
    main()

