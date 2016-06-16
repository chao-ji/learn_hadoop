Compute estimated parameters of Naive Bayes Classifier from synthetic data .

Input: 
  Y: 0, 1
  X = [X0, X1]
  
  X ~ Norm([0, 0], [[1, 0], [0, 5]]), y = 1
  X ~ Norm([3, 5], [[8, 0], [0, 10]]), y = 0
  
  Each class contains 5000 examples described by X = [X0, X1]
  
Output:
  1) Mean (miu) and Variance (sigma^2) of p(X0|Y = 0), p(X1|Y = 0), p(X0|Y = 1), p(X1|Y = 1)

  0_0	mean = 3.028259755199995	var = 7.863774080591995       
  0_1	mean = 0.00854252660000002	var = 1.0022098916146032
  1_0	mean = 4.995530689000024	var = 9.996135126768905
  1_1	mean = -0.08422629199999988	var = 5.211403851660242  

  For example, "0_1" indicates feature X0 and class label 1.

  2) Class prior p(y = 1) and p(y = 0)
  1	0.5
  0	0.5
