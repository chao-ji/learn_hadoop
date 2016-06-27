An alternative implementation of matrix multiplication

Consider an example:

Matrix A:
A1  a b
A2  c d

Matrix B:
B1  e f
B2  g h

Mapper1:
Elements in Matrix A ==>
("1", "a:A1")
("2", "b:A1")
("1", "c:A2")
("2", "d:A2")

Keyed on col index of A

Elements in Matrix B ==>
("1", "e:B1")
("1", "f:B2")
("2", "g:B1")
("2", "h:B2")

Keyed on row index of B

Reducer1:
(1, ["a:A1", "c:A2", "e:B1", "f:B2"])
(2, ["b:A1", "d:A2", "g:B1", "h:B2"])

Divide the values in the list into listA and listB, and then compute the Cartisian Product:


("1:1", "ae")   i.e. Keyed on "rowID:colID" where rowID is a row ID of 'a', and colID is the col ID of 'e'
("1:2", "af")
("2:1", "ce")
("2:2", "cf")

("1:1", "bg")
("1:2", "bh")
("2:1", "dg")
("2:2", "dh")

All we need to do is merge/sum the values based on the common key (rowID:colID) in the resulting matrix C = A*B.
