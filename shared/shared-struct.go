package shared

import "time"

const TIMEFMT = "2006-01-02 15:04:05"

// the log request format
type LogRequest struct {
	RegEx    string // regular expression
	Filename string // the name of log file
}

// the log response format from the server
type LogReply struct {
	Result string // the content of log
	Count  int    // word count
}

type GossipMsg struct {
	Msg       string    // joining or leaving
	Id        int       //unique id for every IP address
	Ip        string    //the target ip address
	TimeStamp time.Time //the time stamp of IP
}

type Member struct {
	Id              int //unique id for every IP address
	Ip              string
	TimeStamp       time.Time
	Status          int // "online-1" or "offline-0"
	UnresponseCount int // count the times of unresposing
}

/* note: GossipRpl is only for new node joining the network.
This structure
can only be sent from introducing node
*/
type GossipRpl struct {
	MemberList []Member
}

// Ask for membership list of own hostConfig
type EmptyReq struct {
}

// Reply membership list
type MembershipRpl struct {
	MemberList []byte
}

// SDFS command
type SDFSMsg struct {
	Type          string
	LocalFileName string
	SDFSFileName  string
	NumVersions   string
	TimeStamp     time.Time
}

type CraneMsg struct {
	AppName string
}

type WriteAck struct {
	Finish bool
}

// for "store"
type SFileList struct {
	SFile map[string][]time.Time
}

// for "ls sdfsfilename"
type DupList struct {
	Nodes []int
}

// for "start app1"
type App struct {
	AppName    string
	Period     time.Duration
	SendPeriod time.Duration
}
