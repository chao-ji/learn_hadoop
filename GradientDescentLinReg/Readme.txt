Performs gradient descent for Least Squere Linear Regression (without regularization).

Input:
  -Data matrix: X, n-by-p
  -Target vector: Y, n-by-1
  -Initial weight vector: ALPHA, p-by-1
  
Output:
  -Weight vector: ALPHA, p-by-1
  
Algorithm:
  -Gradient descent search for ALPHA that minimizes the summed squared errors
  -Update is computed by summing the errors over all training data (i.e. batch gradient descent as opposed to stochastic gradient descent)
  
  The weight component 'j' of alpha is updated as follows: 
  $\alpha_{j}^{t + 1} = \alpha_{j}^{t} + \eta \cdot \sum_{i = 1}^{n} ((y_{i} - \sum_{j = 1}^{p} x_{ij} \cdot \alpha_{j}^{t}) \cdot x_{ij})$
  where $\eta$ equals the learning rate.
  
  In vector representation:
  ALPHA := ALPHA +  X' * (Y - X * ALPHA) * eta
  
  Each iteration is implmented using 6 MapReduce jobs
  
