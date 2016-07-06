Perform Breadth First Search

Input:
  -Undirected graph
  -Represented by list of edges
  -<node p><tab><node q>
  
Output:
  -List of nodes in ascending order of the distance to the source node
  -<node><tab><dist>
  
Algorithm
  I. Prepare the data in appropriate format:
    <node><tab><dist><tab><list of nodes separated by comma>
    
    <dist> = "0" for source node, or "I" otherwise (i.e. infinite)
    
  II. Performs BFS
    Mapper:
    
    For each <node p> whose <dist> = n (not "I")
      Emit (n + 1, "D" + <node q>) for each of the nodes in the adjacency list of <node p>
    Emit (<node p>, "N" + <list of nodes>)
    Emit (<node p>, "D" + <dist>)
    
    Use "D" or "N" to indicate "distance" or "node".
    
    Reducer:
      Compute the minimum value of distances over the list of distances (i.e. integers or "I"s)
      
      Output the result in the original format: <node><tab><dist><tab><list of nodes separated by comma>
      
  III. Output result
    1. List of (node, dist) are keyed on the dist, which are sorted by the MapReduce framework
    
    2. In the reducer, emit (node, dist)

    3. In this way, nodes are sorted in ascending order of the distance
    
    
