package chord

import (
	"dht/internal"
	"fmt"

	"github.com/sirupsen/logrus"
)

type FindSuccessorRequest struct {
	ID  uint32
	TTL int16
}

func (n *ChordNode) FindSuccessor(request FindSuccessorRequest, reply *string) error {
	succ := n.getOnlineSucc()
	if succ == nil || !succ.isConnected() {
		err := fmt.Errorf("%s: no online successor", n.Addr)
		logrus.Error(n.Addr, " FindSucessor: ", err)
		return err
	}
	defer succ.close()
	if inRange(n.Id+1, succ.id+1, request.ID) {
		*reply = succ.remoteAddr
		logrus.Info(n.Addr, " FindSuccessor: request for ", request.ID, " resolved with addr ", succ.remoteAddr)
		return nil
	}
	if request.TTL == 1 {
		err := fmt.Errorf(" request for %d redirected too many times ", request.ID)
		logrus.Error(n.Addr, " FindSuccessor: ", err)
		return err
	}
	fin := n.closestPrecedingFinger(request.ID)
	if fin == nil || fin.remoteAddr == n.Addr {
		logrus.Warn(n.Addr, " FindSuccessor: unable to find finger, use succ")
		fin = succ
	}
	// logrus.Info(n.Addr, " FindSuccessor: redirecting ", request.ID, " to ", fin.remoteAddr)
	return fin.FindSuccessor(request.ID, request.TTL-1, reply)
}

func (n *ChordNode) GetPredecessor(_ string, addr *string) error {
	n.predecsorLock.RLock()
	defer n.predecsorLock.RUnlock()
	if n.predecessor.isConnected() {
		*addr = n.predecessor.remoteAddr
		return nil
	}
	err := fmt.Errorf("no immediate predecessor")
	logrus.Warnf("%s GetPredecessor: %s", n.Addr, err.Error())
	return err
}

func (n *ChordNode) Ping(request int32, reply *int32) error {
	if request != 114514 {
		return fmt.Errorf("%s Ping: suspicious request", n.Addr)
	}
	*reply = 1919810
	return nil
}

func (n *ChordNode) Notify(request string, _ *int8) error {
	id := internal.Str_uint32_sha1(request)
	n.predecsorLock.Lock()
	defer n.predecsorLock.Unlock()
	if !n.predecessor.isConnected() || inRange(n.predecessor.id+1, n.Id, id) {
		logrus.Info(n.Addr, " Notify: being notified new predecessor: ", request)
		n.predecessor.close()
		err := n.predecessor.Dial(request)
		if err != nil {
			n.predecessor.close()
			logrus.Error(n.Addr, " Notify: dial error: ", err)
			return err
		}
		go n.fetchBackupData()
		n.dataLock.Lock()
		for k := range n.data {
			dataID := internal.Str_uint32_sha1(k)
			if inRange(n.Id+1, n.predecessor.id+1, dataID) {
				delete(n.data, k)
			}
		}
		n.dataLock.Unlock()
	}
	return nil
}

func (n *ChordNode) GetSuccList(_ int8, succList *[ChordK]string) error {
	n.succListLock.RLock()
	*succList = n.succList
	n.succListLock.RUnlock()
	return nil
}

func (n *ChordNode) GetAllData(isBackup bool, data *map[string]string) error {
	if isBackup {
		n.backupDataLock.RLock()
		*data = n.backupData
		n.backupDataLock.RUnlock()
	} else {
		n.dataLock.RLock()
		*data = n.data
		n.dataLock.RUnlock()
	}
	return nil
}

func (n *ChordNode) GetDataByKey(key string, value *string) error {
	var ok bool
	n.dataLock.RLock()
	*value, ok = n.data[key]
	n.dataLock.RUnlock()
	if ok {
		return nil
	} else {
		err := fmt.Errorf("unknown key %s", key)
		logrus.Error(n.Addr, " GetDataByKey: ", err)
		return err
	}
}

type PutDataRequest struct {
	IsBackup   bool
	Key, Value string
}

func (n *ChordNode) PutData(request PutDataRequest, ok *bool) error {
	if request.IsBackup {
		n.backupDataLock.Lock()
		n.backupData[request.Key] = request.Value
		n.backupDataLock.Unlock()
	} else {
		n.dataLock.Lock()
		n.data[request.Key] = request.Value
		n.dataLock.Unlock()
		go func() {
			succ := n.getOnlineSucc()
			if succ == nil {
				return
			}
			err := succ.PutData(request.Key, request.Value, true)
			succ.close()
			if err != nil {
				logrus.Error(n.Addr, " PutData: send succ backup KV: ", err)
			}
		}()
	}
	*ok = true
	return nil
}

func (n *ChordNode) SendBackupData(data map[string]string, ok *bool) error {
	n.backupDataLock.Lock()
	for k, v := range data {
		n.backupData[k] = v
	}
	n.backupDataLock.Unlock()
	return nil
}

type DeleteDataRequest struct {
	IsBackup bool
	Key      string
}

func (n *ChordNode) DeleteData(request DeleteDataRequest, ok *bool) error {
	if request.IsBackup {
		n.backupDataLock.Lock()
		delete(n.backupData, request.Key)
		n.backupDataLock.Unlock()
	} else {
		n.dataLock.Lock()
		delete(n.data, request.Key)
		n.dataLock.Unlock()
		go func() {
			succ := n.getOnlineSucc()
			if succ == nil {
				return
			}
			err := succ.DeleteData(request.Key, true)
			succ.close()
			if err != nil {
				logrus.Error(n.Addr, " DeleteData: delete succ backup KV: ", err)
			}
		}()
	}
	*ok = true
	return nil
}

type SuccInformExitRequest struct {
	Addr, PreAddr string
	Data          map[string]string
}

func (n *ChordNode) SuccInformExit(request SuccInformExitRequest, ok *bool) error {
	logrus.Infof("%s SuccInformExit: %s %s", n.Addr, request.Addr, request.PreAddr)
	n.predecsorLock.Lock()
	if request.Addr == n.predecessor.remoteAddr {
		n.predecessor.close()
		err := n.predecessor.Dial(request.PreAddr)
		if err != nil {
			logrus.Error(n.Addr, " SuccInformExit: dialing new predecessor failed with ", err)
			n.predecessor.close()
		}
		n.predecsorLock.Unlock()
		go n.fetchBackupData()
	} else {
		n.predecsorLock.Unlock()
		return nil
	}
	n.dataLock.Lock()
	for k, v := range request.Data {
		n.data[k] = v
	}
	n.dataLock.Unlock()
	go func(data *map[string]string) {
		succ := n.getOnlineSucc()
		if err := succ.SendBackupData(data); err != nil {
			logrus.Error(n.Addr, " SuccInformExit: failed to send backup data to ", succ.remoteAddr, " with error: ", err)
		}
		succ.close()
	}(&request.Data)
	return nil
}

type PredInformExitRequest struct {
	Addr, SuccAddr string
}

func (n *ChordNode) PredInformExit(request PredInformExitRequest, ok *bool) error {
	logrus.Infof("%s PredInformExit: %s %s", n.Addr, request.Addr, request.SuccAddr)
	n.succListLock.Lock()
	if n.succList[0] == request.Addr {
		n.succList[0] = request.SuccAddr
	}
	n.succListLock.Unlock()
	return nil
}
