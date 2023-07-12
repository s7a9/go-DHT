package chord

import (
	"dht/internal"

	"github.com/sirupsen/logrus"
)

// Impl. of DHT interface

func (n *ChordNode) Run() {
	go n.RunRPCServer()
}

func (n *ChordNode) Create() {
	n.succList[0] = n.Addr
	n.fingers[0].Dial(n.Addr)
	n.predecessor.Dial(n.Addr)
	logrus.Infof("%s, %d Join new network", n.Addr, n.Id)
	n.online.Store(true)
	n.maintain()
}

func (n *ChordNode) Join(addr string) bool {
	logrus.Infof("%s, %d Join %s ...", n.Addr, n.Id, addr)
	n.fingersLock.Lock()
	defer n.fingersLock.Unlock()
	link := &n.fingers[0]
	err := link.Dial(addr)
	if err != nil {
		logrus.Error(n.Addr, " Join: fialed to dial ", addr, err)
		return false
	}
	var succAddr string
	err = link.FindSuccessor(n.Id, ChordTTL, &succAddr)
	link.close()
	if err != nil {
		logrus.Error(n.Addr, " Join: failed in FindSuccessor ", err)
		return false
	}
	err = link.Dial(succAddr)
	if err != nil {
		logrus.Error(n.Addr, " Join: fail to dial successor ", succAddr, err)
		return false
	}
	if n.Id == link.id {
		logrus.Error(n.Addr, " Join: conflict ID with ", link.remoteAddr, " , ", n.Id)
		link.close()
		return false
	}
	var newSuccList [ChordK]string
	err = link.GetSuccList(&newSuccList)
	if err != nil {
		logrus.Error(n.Addr, " Join: get succList failed with ", err)
		return false
	}
	n.succListLock.Lock()
	n.succList[0] = succAddr
	for i := 1; i < ChordK; i++ {
		n.succList[i] = newSuccList[i-1]
	}
	n.succListLock.Unlock()
	var data map[string]string
	err = link.GetAllData(&data)
	if err != nil {
		logrus.Error(n.Addr, " Join: fail to get data from successor ", err)
		return false
	}
	n.dataLock.Lock()
	for k, v := range data {
		dataID := internal.Str_uint32_sha1(k)
		if inRange(link.id+1, n.Id+1, dataID) {
			n.data[k] = v
		}
	}
	n.dataLock.Unlock()
	n.online.Store(true)
	n.maintain()
	return true
}

func (n *ChordNode) Quit() {
	if !n.online.Load() {
		return
	}
	logrus.Info(n.Addr, " start Quit")
	n.online.Store(false)
	succ := n.getOnlineSucc()
	if succ != nil {
		if succ.remoteAddr != n.Addr {
			succ.SuccInformExit(n.Addr, n.predecessor.remoteAddr, &n.data)
		}
		n.predecsorLock.RLock()
		if n.predecessor.isConnected() && n.predecessor.remoteAddr != n.Addr {
			n.predecessor.PredInformExit(n.Addr, succ.remoteAddr)
		}
		n.predecsorLock.RUnlock()
		succ.close()
	} else {
		logrus.Error(n.Addr, " Quit: failed to get online")
	}
	n.listener.Close()
	n.CloseRPCLinks()
	n.Clear()
}

func (n *ChordNode) ForceQuit() {
	if !n.online.Load() {
		return
	}
	logrus.Warn(n.Addr, " start ForceQuit")
	n.online.Store(false)
	if err := n.listener.Close(); err != nil {
		logrus.Error(n.Addr, " ForceQuit: close listener with error: ", err)
	}
	n.CloseRPCLinks()
	n.Clear()
}

func (n *ChordNode) Put(key string, value string) bool {
	targetID := internal.Str_uint32_sha1(key)
	var link chordLink
	var targetAddr string
	err := n.FindSuccessor(FindSuccessorRequest{targetID, ChordTTL}, &targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Put: failed in FindSuccessor ", err)
		return false
	}
	logrus.Infof("%s Put: putting %s [%d] to %s", n.Addr, key, targetID, targetAddr)
	err = link.Dial(targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Put: failed to dial target ", err)
		return false
	}
	err = link.PutData(key, value, false)
	if err != nil {
		logrus.Error(n.Addr, " Put: failed to put data ", err)
		return false
	}
	return true
}

func (n *ChordNode) Get(key string) (bool, string) {
	targetID := internal.Str_uint32_sha1(key)
	var value, targetAddr string
	err := n.FindSuccessor(FindSuccessorRequest{targetID, ChordTTL}, &targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Get: failed in FindSuccessor ", err)
		return false, ""
	}
	logrus.Info(n.Addr, " Get: asking ", targetAddr, " for key ", key, " ", targetID)
	var link chordLink
	err = link.Dial(targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Get: failed to dial target ", err)
		return false, ""
	}
	value, err = link.GetDataByKey(key)
	if err != nil {
		logrus.Error(n.Addr, " Get: failed to get data ", err)
		return false, ""
	}
	return true, value
}

func (n *ChordNode) Delete(key string) bool {
	targetID := internal.Str_uint32_sha1(key)
	var targetAddr string
	err := n.FindSuccessor(FindSuccessorRequest{targetID, ChordTTL}, &targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Delete: failed in FindSuccessor ", err)
		return false
	}
	logrus.Info(n.Addr, " Delete: asking ", targetAddr, " to delete key ", key, " ", targetID)
	var link chordLink
	err = link.Dial(targetAddr)
	if err != nil {
		logrus.Error(n.Addr, " Delete: failed to dial target ", err)
		return false
	}
	err = link.DeleteData(key, false)
	if err != nil {
		logrus.Error(n.Addr, " Delete: failed to delete data ", err)
		return false
	}
	return true
}
