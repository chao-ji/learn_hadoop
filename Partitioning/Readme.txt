Partition the result into different groups

Input:
  -List of nodes from the BFS example: https://github.com/chao-ji/learn_hadoop/tree/master/BreadthFirstSearch
  -Format: <node><tab><dist>
  
Output:
  -Input file is partitioned into different part files (part-r-0000n) grouped by the <dist>

Implementation:
  -Create customized partitioner class that generates the <dist> as the partition number
