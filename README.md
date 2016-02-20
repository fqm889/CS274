## Scheduler 

Scheduler is the main class of this program. The entrance of the program is Scheduler.main, 
which allocates an instance of scheduler. The scheduler then executes its algorithm. 

In the scheduler object, a receiver and a sender are allocated, 
for receiving requests from and sending requests to other DC. 
A client is allocated, which is to receive requests from YCSB.

The scheduler keeps book of its log and its different kinds of pending transactions, 
which varies depending on which model this scheduler is implemented according to.

Processors process log and PT, and the algorithms are executed in those classes.

## Dataflow

YCSB  -->  Client  ----ClientReq---->  Scheduler  -----Txns----->  Log

DC  -----String----->  Server  -----ServerReq----->  Scheduler  -----Txns---->  PT

Scheduler  -----ServerReq----->  Server  -----String----->  DC

Log  -----Txns----->  Operation  ---->  Hbase