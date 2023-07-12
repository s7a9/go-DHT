package chord

import (
	"dht/internal"
	"errors"
	"net"
	"net/rpc"
	"time"
)

func (l *chordLink) Dial(addr string) error {
	// logrus.Infof("Connecting to %s", addr)
	l.remoteAddr = addr
	l.id = internal.Str_uint32_sha1(addr)
	var err error
	conn, err := net.DialTimeout("tcp", addr, time.Second*10)
	if err != nil {
		// logrus.Error("Dial:", err)
		return err
	}
	l.rpcClient = rpc.NewClient(conn)
	return nil
}

func (link *chordLink) Call(method string, args interface{}, reply interface{}) error {
	const NodeServName = "ChordNode."
	// logrus.Infof("Call %s %s %v", link.remoteAddr, method, args)
	err := link.rpcClient.Call(NodeServName+method, args, reply)
	return err
}

func (link *chordLink) FindSuccessor(id uint32, ttl int16, addr *string) error {
	return link.Call("FindSuccessor", FindSuccessorRequest{
		ID:  id,
		TTL: ttl,
	}, addr)
}

func (link *chordLink) GetPredecessor(addr *string) error {
	return link.Call("GetPredecessor", "", addr)
}

// Test whether remote machine can response normally. If not, the link would be closed.
func (link *chordLink) Ping() (int32, error) {
	var request, reply int32 = 114514, 0
	err := link.Call("Ping", request, &reply)
	if err != nil {
		link.close()
		return 0, err
	}
	if reply != 1919810 {
		link.close()
		return 0, errors.New("Ping: suspicious reply")
	}
	return reply, nil
}

func (link *chordLink) Notify(addr string) error {
	var tmp int8
	return link.Call("Notify", addr, &tmp)
}

func (link *chordLink) GetSuccList(succList *[ChordK]string) error {
	var tmp int8
	return link.Call("GetSuccList", tmp, succList)
}

func (link *chordLink) GetAllData(data *map[string]string) error {
	return link.Call("GetAllData", false, data)
}

func (link *chordLink) GetDataByKey(key string) (string, error) {
	var value string
	err := link.Call("GetDataByKey", key, &value)
	return value, err
}

func (link *chordLink) GetBackupData(backup *map[string]string) error {
	return link.Call("GetAllData", true, backup)
}

func (link *chordLink) PutData(key, value string, isBackup bool) error {
	var ok bool
	return link.Call("PutData", PutDataRequest{
		IsBackup: isBackup,
		Key:      key,
		Value:    value,
	}, &ok)
}

func (link *chordLink) SendBackupData(data *map[string]string) error {
	var ok bool
	return link.Call("SendBackupData", *data, &ok)
}

func (link *chordLink) DeleteData(key string, isBackup bool) error {
	var ok bool
	return link.Call("DeleteData", DeleteDataRequest{
		IsBackup: isBackup,
		Key:      key,
	}, &ok)
}

func (link *chordLink) SuccInformExit(addr, preAddr string, data *map[string]string) {
	var ok bool
	link.Call("SuccInformExit", SuccInformExitRequest{
		Addr:    addr,
		PreAddr: preAddr,
		Data:    *data,
	}, &ok)
}

func (link *chordLink) PredInformExit(addr, succAddr string) {
	var ok bool
	link.Call("PredInformExit", PredInformExitRequest{
		Addr:     addr,
		SuccAddr: succAddr,
	}, &ok)
}
