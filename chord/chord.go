package chord

import (
	"dht/internal"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	f, _ := os.Create("dht_chord.log")
	logrus.SetOutput(f)
}

const (
	ChordM            = 32
	ChordK            = 6
	ChordTTL          = 50
	stabilizeInterval = time.Millisecond * 200
	fixFingerInterval = time.Millisecond * 200
	fixPredInterval   = time.Millisecond * 200
)

// inRange judges whether id is in range [start, end) on the circle
func inRange(start, end, id uint32) bool {
	if start < end {
		return start <= id && id < end
	} else {
		return start <= id || id < end
	}
}

type chordLink struct {
	id         uint32
	remoteAddr string
	rpcClient  *rpc.Client
}

func (l *chordLink) isConnected() bool {
	return l.rpcClient != nil
}

func (l *chordLink) close() {
	if l.isConnected() {
		l.id = 0
		l.remoteAddr = ""
		l.rpcClient.Close()
		l.rpcClient = nil
	}
}

type ChordNode struct {
	Id        uint32
	Addr      string
	curFinger uint16
	online    atomic.Bool

	listener net.Listener
	server   *rpc.Server

	data     map[string]string
	dataLock sync.RWMutex

	backupData     map[string]string
	backupDataLock sync.RWMutex

	fingers     [ChordM]chordLink
	fingersLock sync.RWMutex

	predecessor   chordLink
	predecsorLock sync.RWMutex

	succList     [ChordK]string
	succListLock sync.RWMutex

	activeConn     map[net.Conn]struct{}
	activeConnLock sync.Mutex
}

// local methods

func CreateChordNode(addr string) *ChordNode {
	return &ChordNode{
		Addr:       addr,
		Id:         internal.Str_uint32_sha1(addr),
		data:       make(map[string]string),
		backupData: make(map[string]string),
		activeConn: make(map[net.Conn]struct{}),
	}
}

func (n *ChordNode) Clear() {
	n.Id = 0
	n.Addr = ""
	n.curFinger = 0
	n.server = nil
	n.data = make(map[string]string)
	n.backupData = make(map[string]string)
	n.activeConn = make(map[net.Conn]struct{})
}

func (n *ChordNode) RunRPCServer() {
	n.server = rpc.NewServer()
	n.server.Register(n)
	var err error
	n.listener, err = net.Listen("tcp", n.Addr)
	if err != nil {
		logrus.Error(n.Addr, " listen error: ", err)
	}
	n.online.Store(true)
	for n.online.Load() {
		conn, err := n.listener.Accept()
		if err != nil {
			logrus.Warn(n.Addr, " accpet: ", err)
			continue
		}
		go func(conn net.Conn) {
			n.activeConnLock.Lock()
			n.activeConn[conn] = struct{}{}
			n.activeConnLock.Unlock()
			n.server.ServeConn(conn)
			n.activeConnLock.Lock()
			delete(n.activeConn, conn)
			n.activeConnLock.Unlock()
		}(conn)
	}
}

func (n *ChordNode) CloseRPCLinks() {
	n.fingersLock.Lock()
	for i := 0; i < len(n.fingers); i++ {
		n.fingers[i].close()
	}
	n.fingersLock.Unlock()
	n.predecsorLock.Lock()
	n.predecessor.close()
	n.predecsorLock.Unlock()
	n.activeConnLock.Lock()
	for k := range n.activeConn {
		k.Close()
	}
	n.activeConnLock.Unlock()
}

func (n *ChordNode) maintain() {
	go func() {
		for n.online.Load() {
			n.stabilize()
			time.Sleep(stabilizeInterval)
		}
	}()
	go func() {
		for n.online.Load() {
			n.fixFingers()
			time.Sleep(fixFingerInterval)
		}
	}()
	go func() {
		for n.online.Load() {
			n.fixPredecessor()
			time.Sleep(fixPredInterval)
		}
	}()
}

func (n *ChordNode) stabilize() {
	succ := n.getOnlineSucc()
	if succ == nil {
		logrus.Warn(n.Addr, " stabilize: no online successor ")
		return
	}
	var succAddr string
	err := succ.GetPredecessor(&succAddr)
	if err != nil {
		if err.Error() == "no immediate predecessor" {
			succAddr = succ.remoteAddr
			logrus.Warn(n.Addr, " stabilize: get possible succ addr failed: ", err, " try original succ")
		} else {
			logrus.Error(n.Addr, " stabilize: failed to get possible succ")
			return
		}
	}
	succID := internal.Str_uint32_sha1(succAddr)
	// logrus.Infof("%s stabilize: possible succ: %s %d", n.Addr, succAddr, succID)
	if inRange(n.Id+1, succ.id, succID) {
		logrus.Infof("%s stabilize: closer succ: %s %d", n.Addr, succAddr, succID)
		succ.close()
		err = succ.Dial(succAddr)
		if err != nil {
			logrus.Error(n.Addr, " stabilize: succ dial error: ", err)
			return
		}
	} else { // succ remain unchanged
		succAddr = succ.remoteAddr
		succID = succ.id
	}
	if succ.remoteAddr == n.Addr {
		logrus.Info(n.Addr, " stabilize: succ is self")
		return
	}
	err = succ.Notify(n.Addr)
	if err != nil {
		logrus.Error(n.Addr, " stabilize: notify failed with ", err)
	}
	var newSuccList [ChordK]string
	err = succ.GetSuccList(&newSuccList)
	if err != nil {
		logrus.Error(n.Addr, " stabilize: get succList failed with ", err)
		return
	}
	n.fingersLock.Lock()
	if n.fingers[0].id == succID {
		succ.close()
	} else { // move succ to fingers[0]
		n.fingers[0].close()
		n.fingers[0] = *succ
	}
	n.fingersLock.Unlock()
	n.succListLock.Lock()
	n.succList[0] = succAddr
	for i := 1; i < ChordK && newSuccList[i-1] != ""; i++ {
		n.succList[i] = newSuccList[i-1]
	}
	logrus.Info(n.Addr, " stabilize: new succ list ", n.succList)
	n.succListLock.Unlock()
}

func (n *ChordNode) fixFingers() {
	var startID uint32 = n.Id + (1 << n.curFinger)
	var addr string
	err := n.FindSuccessor(FindSuccessorRequest{startID, ChordTTL}, &addr)
	if err != nil {
		logrus.Errorf("%s fixFingers: fialed to find successor of %d: %s", n.Addr, startID, err)
		return
	}
	finger := &n.fingers[n.curFinger]
	n.fingersLock.Lock()
	if finger.remoteAddr != addr {
		finger.close()
		err := finger.Dial(addr)
		if err != nil {
			logrus.Error(n.Addr, " fixFingers: fail to dial ", err)
		}
	}
	n.fingersLock.Unlock()
	n.curFinger = (n.curFinger + 1) % ChordM
	if n.curFinger == 0 {
		n.curFinger = 1
	}
}

func (n *ChordNode) fetchBackupData() {
	newBackup := make(map[string]string)
	n.predecsorLock.RLock()
	defer n.predecsorLock.RUnlock()
	if !n.predecessor.isConnected() {
		logrus.Warn(n.Addr, " fetchBackupData: predecessor not connected")
		return
	}
	if err := n.predecessor.GetAllData(&newBackup); err != nil {
		logrus.Error(n.Addr, " fetchBackupData: get backup data from ", n.predecessor.remoteAddr, " failed with ", err)
		return
	}
	n.backupDataLock.Lock()
	n.backupData = newBackup
	n.backupDataLock.Unlock()
}

func (n *ChordNode) fixPredecessor() {
	n.predecsorLock.Lock()
	defer n.predecsorLock.Unlock()
	if !n.predecessor.isConnected() {
		return
	}
	if _, err := n.predecessor.Ping(); err != nil {
		logrus.Warn(n.Addr, " fixPredecessor: predecessor disconnected: ", err)
		n.backupDataLock.RLock()
		n.dataLock.Lock()
		for k, v := range n.backupData {
			n.data[k] = v
		}
		n.dataLock.Unlock()
		n.backupDataLock.RUnlock()
		succ := n.getOnlineSucc()
		if succ != nil {
			err = succ.SendBackupData(&n.backupData)
			if err != nil {
				logrus.Error(n.Addr, " fixPredecessor: failed to send backup data ", err)
			}
		}
	} else {
		logrus.Infof("%s fixPredecessor: %s OK", n.Addr, n.predecessor.remoteAddr)
	}
}

func (n *ChordNode) getOnlineSucc() *chordLink {
	n.succListLock.RLock()
	defer n.succListLock.RUnlock()
	for _, addr := range n.succList {
		if addr == "" {
			continue
		}
		link := &chordLink{}
		err := link.Dial(addr)
		if err == nil {
			return link
		}
		logrus.Warn(n.Addr, " getOnlineSucc: failed to connect to succ ", addr, " : ", err)
	}
	return nil
}

func (n *ChordNode) closestPrecedingFinger(id uint32) *chordLink {
	n.fingersLock.Lock()
	defer n.fingersLock.Unlock()
	for i := ChordM - 1; i >= 0; i-- {
		fin := &n.fingers[i]
		if !fin.isConnected() {
			continue
		}
		if _, err := fin.Ping(); err != nil {
			fin.close()
			continue
		}
		if inRange(n.Id, id, fin.id) {
			return fin
		}
	}
	return nil
}
