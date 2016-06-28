Compute average/mean of a set of numbers using Combiner.

Map:
  input:
    key: N/A
    value: 'num' read from a line of text
  output:
    key: "key"
    value: MeanCountTuple (implements Writable) containing the running mean 'mean' (e.g. 'num') and 'count' (e.g. 1)

Reduce:
  input:
    key: "key"
    value: MeanCountTuple (implements Writable) containing 1) mean 2) count
  output:
    key: "key"
    value: MeanCountTuple containing 
      1) running mean 'mean', computed as the sum / count, where sum is aggregated over 
      the product of 'mean' and 'count'
      2) running count 'count'
      
  NOTE: This Reducer can be used as combiner.    
