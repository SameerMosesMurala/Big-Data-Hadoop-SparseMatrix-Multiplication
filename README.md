Project Description
For this project, you are asked to implement matrix multiplication in Map-Reduce. 
Do not use the method in Section 2.3.10. You should modify Multiply.java only. In your Java main program, args[0] is the first
input matrix M, args[1] is the second input matrix N, args[2] is the directory name to pass the intermediate results from the 
first Map-Reduce job to the second, and args[3] is the output directory. There are two small sparce matrices 4*3 and 3*3 in the files M-matrix-small.txt 
and N-matrix-small.txt for testing in standalone mode. Their matrix multiplication must return the 4*3 matrix in solution-small.txt.
Then there are 2 moderate-sized matrices 200*100 and 100*300 in the files M-matrix-large.txt and M-matrix-large.txt for testing in 
distributed mode. Their matrix multiplication must return the matrix in solution-large.txt.

You can compile Multiply.java using:

run multiply.build

Pseudo-Code
To help you, I am giving you the pseudo code:

class Elem extends Writable {
  short tag;  // 0 for M, 1 for N
  int index;  // one of the indexes (the other is used as a key)
  double value;
  ...
}

class Pair extends WritableComparable<Pair> {
  int i;
  int j;
  ...
}
(Add methods toString so you can print Elem and Pair.) First Map-Reduce job:
map(key,line) =             // mapper for matrix M
  split line into 3 values: i, j, and v
  emit(j,new Elem(0,i,v))

map(key,line) =             // mapper for matrix N
  split line into 3 values: i, j, and v
  emit(i,new Elem(1,j,v))

reduce(index,values) =
  A = all v in values with v.tag==0
  B = all v in values with v.tag==1
  for a in A
     for b in B
         emit(new Pair(a.index,b.index),a.value*b.value)
Second Map-Reduce job:
map(key,value) =  // do nothing
  emit(key,value)

reduce(pair,values) =  // do the summation
  m = 0
  for v in values
    m = m+v
  emit(pair,m)
