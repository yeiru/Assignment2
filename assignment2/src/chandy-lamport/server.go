package chandy_lamport

import (
	"log"
	"fmt"
	"strconv"
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
	inSnapshotProcess bool
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
		false,
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
	
	// TokenMessage / MarkerMessage
	server.sim.logger.RecordEvent(server, ReceivedMessageEvent{src, server.Id, message})
	fmt.Println("****************************")
	var snapshotId int
	snapshotId = -1
	switch messageType := message.(type) {
	case TokenMessage:
		
			fmt.Printf("Received Tokens from %s in %s", src, server.Id)
			server.Tokens += messageType.numTokens
			fmt.Println("Number of tokens")
			fmt.Println(server.Tokens)
			//fmt.Println("tokens in channel")
			if server.inSnapshotProcess {
				server.updateInboundChannel(src, messageType)				
			}
			

	case MarkerMessage:
		fmt.Println("Received Marker in " + server.Id)
		snapshotId = messageType.snapshotId
		if _, ok := server.alreadySnapshottedMap[snapshotId]; ok {
			
			fmt.Println("Already snapshotted")
			server.populateSnapshotMessages(snapshotId, src)
			if server.snapshotCompletedForServer(snapshotId){
				fmt.Println("All markers arrived, notify simulator")
				server.inSnapshotProcess = false
				server.sim.NotifySnapshotComplete(server.Id, snapshotId)
			}

		} else {
			fmt.Println("Server snapshotted and marker sent")
			server.snapshotServer(snapshotId, src)
			server.SendToNeighbors(MarkerMessage{snapshotId})
		}
	default:
		log.Fatal("Error unknown event: ", messageType)
	}
	fmt.Println("****************************")
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME	
	fmt.Println("Starting Snapshot IN SERVER")
	server.snapshotServer(snapshotId, "")	
	server.SendToNeighbors(MarkerMessage{snapshotId})	
}

func (server *Server) snapshotServer(snapshotId int, src string) {	
	server.inSnapshotProcess = true
	server.SnapshotTokensMap[snapshotId] = server.Tokens
	server.alreadySnapshottedMap[snapshotId] = true	
	for srcKey, _ := range server.outboundLinks {
		fmt.Println("KEY: " + srcKey)
		if src != srcKey {
			queue := NewQueue()
			queue.Push(SnapshotMessage{server.Id, server.Id, MarkerMessage{snapshotId}})
			server.channelsQueue[snapshotId] = make(map[string]*Queue)
			server.channelsQueue[snapshotId][srcKey] = queue
			fmt.Println("Queue created for Server " + server.Id + ", Src" + srcKey)
			server.channelsQueue[snapshotId][srcKey].PrintQueue()
			server.ignoreChannelPerSnapshot[snapshotId] = src
			server.numberOfChannelsToWait[snapshotId]++
		}
	}
	/*if server.numberOfChannelsToWait[snapshotId] == 1 {
		server.numberOfChannelsToWait[snapshotId] = 0
	}*/
}

func (server *Server) snapshotCompletedForServer(snapshotId int) bool {
	/*isEmpty := false
	for _, queue := range server.channelsQueue[snapshotId] {
		if !queue.Empty() {
			isEmpty = true
		}
	}
	return isEmpty*/

	return (server.numberOfChannelsToWait[snapshotId] == 0)
	
}

func (server *Server) updateInboundChannel(src string, message interface{}) {
	fmt.Println("IN UPDATE INBOUND CHANNEL")
	/*for snapshotId, _ := range server.channelsQueue {
		fmt.Println("In loop SID " + strconv.Itoa(snapshotId))
		if _, ok := server.channelsQueue[snapshotId]; ok {
			fmt.Println("MAP EXISTS")
			if _, ok := server.channelsQueue[snapshotId][src]; ok {
				fmt.Println("QUEUE EXISTS")
				if server.channelsQueue[snapshotId][src].Empty() {
					fmt.Println("QUEUE EMPTY")
				} else {
					fmt.Println("QUEUE NOT EMPTY")

					server.channelsQueue[snapshotId][src].PrintQueue()
				}
			} else {
				fmt.Println("QUEUE DOES NOT EXIST")				
			}
		} else {
			fmt.Println("MAP DOES NOT EXIST")
		}
		if _, ok := server.channelsQueue[snapshotId][src]; ok && !server.channelsQueue[snapshotId][src].Empty() {
			fmt.Println("Appended message ")
			server.channelsQueue[snapshotId][src].Push(SnapshotMessage{src, server.Id, message})
			//server.channelsQueue[src].printQueue()
		}
	}*/

	
	for snapshotId, snapshotQueues := range server.channelsQueue {
		fmt.Println("In loop SID " + strconv.Itoa(snapshotId))
		/*for _, activeChannel := range server.activeChannelPerSnapshot[snapshotId] {
			fmt.Println(activeChannel)
		}
		for _, activeChannel := range server.activeChannelPerSnapshot[snapshotId] {
			
			if _, ok := snapshotQueues[src]; ok {
				snapshotQueues[src].Push(SnapshotMessage{src, server.Id, message})
				snapshotQueues[src].PrintQueue()
			}

		}*/
		if value, ok := server.ignoreChannelPerSnapshot[snapshotId]; ok && value != src {
			fmt.Println("Source to ignore: " + value)
			for  queueSrc, channelQueue := range snapshotQueues {
					
				
					fmt.Println("IN QUEUE : " + queueSrc)
					channelQueue.Push(SnapshotMessage{src, server.Id, message})
					channelQueue.PrintQueue()
					/*if channelQueue.Empty() {
						fmt.Println("QUEUE EMPTY")
					} else {
						fmt.Println("QUEUE NOT EMPTY")
						
						channelQueue.Push(SnapshotMessage{src, server.Id, message})
						channelQueue.PrintQueue()
					}*/
				
			} 
		}
	}

	/*fmt.Println("In update channels, source " + src)
	fmt.Println("Tokens got: " + strconv.Itoa(tokens))
	fmt.Println("Tokens in map")
	fmt.Println(strconv.Itoa(server.channelTokensMap[0][src]))*/
}

func (queue Queue) printQueue() {
	for !queue.Empty() {
		fmt.Println(queue.Peek())
		queue.Pop()
	}
}

func (server *Server) populateSnapshotMessages(snapshotId int, src string) {
	fmt.Println("Empty Queue and populate Snapshot Messages")
	server.numberOfChannelsToWait[snapshotId]--
	if _, ok := server.channelsQueue[snapshotId]; ok {
		fmt.Println("PSM: Snapshot exists")
		if _, ok := server.channelsQueue[snapshotId][src]; ok {
			fmt.Println("PSM: Queue for Snapshot exists")
			for !server.channelsQueue[snapshotId][src].Empty() {

				e := server.channelsQueue[snapshotId][src].Peek().(SnapshotMessage)	
				server.channelsQueue[snapshotId][src].Pop()		
				fmt.Println(e)
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
