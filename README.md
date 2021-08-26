# Data preparation 
This node is in charge of slicing and distributing the chunks among available data-prep nodes in order to compress them.

### DAG
The following figure shows the Directed Acyclic Graph(DAG) of **N** data preparation nodes which distributed the operations among the available nodes with the objective of achieving 
implicit parallelism.

<p align="center">
  <img width="550" height="350" src="https://i.imgur.com/ShsEclQ.png">
</p>