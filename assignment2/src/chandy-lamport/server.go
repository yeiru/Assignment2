package chandy_lamport

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	//Field for remembering if this has been snapshotted
	SnapshotTokensMap map[int]int
	alreadySnapshottedMap map[int]bool
	inSnapshotProcess int
	channelTokensMap map[int]map[string]int
	channelsQueue map[int]map[string]*Queue
	SnapshotMessages map[int][]*SnapshotMessage
	ignoreChannelPerSnapshot map[int]string
	numberOfChannelsToWait map[int]int
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]int),
		make(map[int]bool),
		0,
		make(map[int]map[string]int),
		make(map[int]map[string]*Queue),
		make(map[int][]*SnapshotMessage),
		make(map[int]string),
		make(map[int]int),	
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	server.sim.logger.RecordEvent(server, ReceivedMessageEvent{src, server.Id, message})
	var snapshotId int
	snapshotId = -1
	switch messageType := message.(type) {
	case TokenMessage:
		server.Tokens += messageType.numTokens
		if server.inSnapshotProcess > 0 {
			server.updateInboundChannel(src, messageType)				
		}
	case MarkerMessage:
		snapshotId = messageType.snapshotId
		if _, ok := server.alreadySnapshottedMap[snapshotId]; ok {			
			server.populateSnapshotMessages(snapshotId, src)
			if server.snapshotCompletedForServer(snapshotId){
				server.inSnapshotProcess--
				server.sim.NotifySnapshotComplete(server.Id, snapshotId)
			}
		} else {
			waitForChannels := server.snapshotServer(snapshotId, src)
			server.SendToNeighbors(MarkerMessage{snapshotId})
			if !waitForChannels {
				server.inSnapshotProcess--
				server.sim.NotifySnapshotComplete(server.Id, snapshotId)
			}
		}
	default:
		log.Fatal("Error unknown event: ", messageType)
	}	
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME	
	server.snapshotServer(snapshotId, "")	
	server.SendToNeighbors(MarkerMessage{snapshotId})		
}

func (server *Server) snapshotServer(snapshotId int, src string) bool {	
	waitForChannels := false;
	server.SnapshotTokensMap[snapshotId] = server.Tokens
	for _, srcKey := range getSortedKeys(server.inboundLinks) {
		if src != srcKey {
			queue := NewQueue()
			queue.Push(SnapshotMessage{server.Id, server.Id, MarkerMessage{snapshotId}})
			server.channelsQueue[snapshotId] = make(map[string]*Queue)
			server.channelsQueue[snapshotId][srcKey] = queue
			server.ignoreChannelPerSnapshot[snapshotId] = src
			server.numberOfChannelsToWait[snapshotId]++
		}
	}	
	
	if server.numberOfChannelsToWait[snapshotId] != 0 {
		server.inSnapshotProcess++		
		server.alreadySnapshottedMap[snapshotId] = true
		waitForChannels = true
	}
	return waitForChannels
}

func (server *Server) snapshotCompletedForServer(snapshotId int) bool {
	return (server.numberOfChannelsToWait[snapshotId] == 0)	
}

func (server *Server) updateInboundChannel(src string, message interface{}) {
	for snapshotId, snapshotQueues := range server.channelsQueue {
		if value, ok := server.ignoreChannelPerSnapshot[snapshotId]; ok && value != src {			
			for  _, channelQueue := range snapshotQueues {	
					channelQueue.Push(SnapshotMessage{src, server.Id, message})
			} 
		}
	}
}

func (server *Server) populateSnapshotMessages(snapshotId int, src string) {
	server.numberOfChannelsToWait[snapshotId]--
	if _, ok := server.channelsQueue[snapshotId]; ok {
		if _, ok := server.channelsQueue[snapshotId][src]; ok {
			for !server.channelsQueue[snapshotId][src].Empty() {
				e := server.channelsQueue[snapshotId][src].Peek().(SnapshotMessage)	
				server.channelsQueue[snapshotId][src].Pop()		
				switch messageType := e.message.(type) {
				case MarkerMessage:
					continue
				case TokenMessage:
					server.SnapshotMessages[snapshotId] = append(server.SnapshotMessages[snapshotId], &e)
				default:
					log.Fatal("Error unknown event: ", messageType)
				}	
			}	
		}
	}
}
