# CS425 MP-4

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
