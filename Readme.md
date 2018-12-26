# Crane Stream Processing system

This is a course project from UIUC cs425 distributed system and is divided into 4 stages:

1. Setting up a distributed log system where we can check and grep logs on multiple machines (mp-1, detail explained in cs425_mp1.pdf)
2. Building a p2p membership system with dynamic membership list. The system support high reliability fault tolerance (mp-2, detail explained in cs425_mp2.pdf)
3. Building a distributed file system (DFS) with replica control. The DFS is tolerant to at most 3 simultaneous fails and any numbers of total fails. (mp-3, detail explained in cs425_mp3.pdf)
4. Building a stream processing system like Apache Storm, which can read file from the DFS, process data in different node with different jobs, and write the temporary result into DFS. The system could guarantee the correctness using acker method like Apache Storm, and is generally faster than Apache Spark with relatively small dataset. (detail explained in cs425_mp4.pdf)

## Command
* ```make clean```: remove all the binary and log files
* ```make build```: build the binarys
* ```make start```: starting the server as a daemon
* ```make stop```: stop the server

## How to Deploy
1. cd to the mp-3 repository
2. ```make clean```
3. ```make start```
4. ```./p2pClient```

Then you can use the client to send request to the local server:

* **start application-name local-data-name**: start an application
* **start application-name local-data-name period sendPeriod**: start an application and specify args
* **put localfilename sdfsfilename**: put local file to SDFS
* **get sdfsfilename localfilename**: fetch sdfsfile from SDFS
* **get-versions sdfsfilename num-versions localfilename**: fetch num-versions sdfsfile from SDFS
* **delete sdfsfilename**: delete sdfsfilename in SDFS
* **ls sdfsfilename**: list all VM addreses where this file is currently being stored
* **store**: list all files currently being stored at this machine
* **join**: join the network (introducer must be in the network!)
* **leave**: voluntarily leave the network.
* **memberlist**: show membership list
* **peerlist**: show peer list
* **showID**: show the ID of the vm
* **grep**: grep the log

Then you can check the log file in *output.log*
