package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"../shared"
)

type Gossip int //receive the gossip message (joining, leaving)

//Handle gossip message from local client or other VMs
func (r *Gossip) RecGossip(args *shared.GossipMsg, reply *shared.GossipRpl) error {
	if args.Ip != SELFIP || ID == 0 {
		// If the sender comes from elsewhere:
		// 1. Update the status
		// 2. Send back the updated memberList if its introducer
		// 3. If updated, gossip the message to peer VM
		//log.Printf("Message about: % d received", args.Id)
		if memberList[ID].Status == 0 && ID != 0 {
			// if offline, discard the message...
			//log.Printf("Offline, discarding the message...")
			return nil
		}

		if !memberList[args.Id].TimeStamp.Before(args.TimeStamp) {
			// if message outdated, discard the message...
			//log.Printf("Outdated Message about ID: %d", args.Id)
			return nil
		}

		// Local state is outdated
		memberList[args.Id].TimeStamp = args.TimeStamp
		if args.Msg == "join" {
			memberList[args.Id].Status = 1
		} else {
			// both "leave" and "fail"
			memberList[args.Id].Status = 0
		}
		//log.Printf("VM <%d> : <%s>!!", args.Id, args.Msg)
		log.Printf("Membership updated\n")
		updatePeerList()
		assignRoles()

		//gossip update to live peers with multiple goroutine
		channel := make(chan *rpc.Call, NUMOFPEER)
		for _, member := range peerList {
			if member.Status == 1 {
				SendGossipAsync(args, member.Ip, channel)
			} else {
				channel <- &rpc.Call{}
			}
		}
		for i := 0; i < NUMOFPEER; i++ {
			gCall := <-channel
			checkErr(gCall.Error)
		}

		// if the server is introducer, then reply full membership list to sender
		if ID == 0 {
			reply.MemberList = memberList[:]
		}

	} else {
		// If the sender is itself (request comes from local):
		// 1. Update the status of itself in the memberList
		// 2. Send "join" to the introducer or send "leave" to the peer VM

		if !memberList[args.Id].TimeStamp.Before(args.TimeStamp) {
			// if message outdated, discard the message...
			return nil
		}
		if args.Msg == "join" {
			memberList[ID].Status = 1
			memberList[ID].TimeStamp = args.TimeStamp
			SendGossipSync(args, memberList[0].Ip)
		} else {
			memberList[ID].Status = 0
			memberList[ID].TimeStamp = args.TimeStamp
			channel := make(chan *rpc.Call, NUMOFPEER)
			for _, member := range peerList {
				if member.Status == 1 {
					SendGossipAsync(args, member.Ip, channel)
				} else {
					channel <- &rpc.Call{}
				}
			}
			for i := 0; i < NUMOFPEER; i++ {
				gCall := <-channel
				checkErr(gCall.Error)
			}
		}
	}
	return nil
}

/* Send the gossip message to certain IP address
SendGossipAsync: send the message asynchronously to the peer nodes
SendGossipSync: send the message synchronously to the peer nodes (used when adding to the network)
*/
//// TODO: client use rpc Call to make server know the application
func SendGossipAsync(args *shared.GossipMsg, ip string, channel chan *rpc.Call) {
	client, err := rpc.Dial("tcp", ip+":"+RPCPORT)

	if err != nil {
		fmt.Println("SendGossipAsync: Message:" + args.Msg + " about " + strconv.Itoa(args.Id) + " to " + ip + " : connection Fail!")
		channel <- &rpc.Call{}
		return
	}
	fmt.Println("SendGossipAsync: Message:" + args.Msg + " about " + strconv.Itoa(args.Id) + " to " + ip + " : connection success...")
	reply := &shared.GossipRpl{}
	gCall := client.Go("Gossip.RecGossip", args, reply, channel)
	checkErr(gCall.Error)
}

func SendGossipSync(args *shared.GossipMsg, ip string) {
	client, err := rpc.Dial("tcp", ip+":"+RPCPORT)
	if err != nil {
		log.Println("SendGossipSync: From " + args.Ip + "to" + ip + " : connection Fail!")
		return
	}
	log.Println("SendGossipSync: From " + args.Ip + "to" + ip + " : connection success...")
	reply := &shared.GossipRpl{}
	err = client.Call("Gossip.RecGossip", args, reply)
	for i, member := range reply.MemberList {
		memberList[i] = member
	}
	printMemberList()
	updatePeerList()
	assignRoles()
	checkErr(err)
}
