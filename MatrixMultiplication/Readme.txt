Performs Matrix multiplication

Input:
  -matrix A (p-by-q):
    A1:2  a b c
    A2:2  d e f
    
  -matrix B (q-by-r):
    B1:2  g h
    B2:2  i j
    B3:2  k l
    
Output:
  -matrix C:
    C1  a*g+b*i+c*k   a*h+b*j+c*l
    C2  d*g+e*i+f*k   d*h+e*j+f*l
    
Algorithm:
  The algorithm consists of 3 MapReduce jobs
  
  Mapper1:
    For each element a_ij in matrix A,
      outputKey = "i:j:k", where k = 1 to r
      outputVal = "a_ij:A:i:j"
    For each element b_jk in matrix B,
      outputKey = "i:j:k", where i = 1 to p
      outputVal = "b_jk:B:j:k"
      
    In other words, each element in A or B are emitted r or p times 
  
  Reducer1:
    inputKey = "i:j:k"
    inputValue = ["a_ij:A:i:j", "b_jk:B:j:k"]
    
    outputKey = "i:k"
    outputValue = a_ij * b_jk
    
  Mapper2:
    inputValue = "i:k a_ij*b_jk"
    
    outputKey = "i:k"
    outputValue = "a_ij*b_jk"
  
  Reducer2:
    inputKey = "i:k"
    intputValue = list of "a_ij*b_jk"
    
    outputKey = "i:k"
    outputValue = sum of list of "a_ij*b_jk"
    
  Job 3 converts the representation of elements in C = A * B  from "C_ik  [sum]" into the natural representation:
  
  C1  C_11  C_12  ...
  C2  C_21  C_22  ...
  ... ...   ...   ...
  
  Mapper3:
    inputValue = "i:k   [sum]"
    
    outputKey = "i"
    outputValue = "[sum]:k"
    
  Reducer3:
    inputKey = "i"
    inputValue = list of "[sum]:k"
    
    outputKey = "C_i"
    outputValue = "C_i1 C_i2 ... C_ik" where the value of each C_ik corresponds to the [sum] in the inputValue 
    
