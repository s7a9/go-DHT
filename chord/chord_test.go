package chord

import (
	"fmt"
	"testing"
	"time"
)

const P = 20000

func makeLocalAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", P+port)
}

func TestSmallNodes(t *testing.T) {
	const N, N1 = 3, 2
	var nodes [N]*ChordNode
	for i := 0; i < N; i++ {
		nodes[i] = CreateChordNode(makeLocalAddr(i))
		nodes[i].Run()
	}
	time.Sleep(200 * time.Millisecond)
	nodes[0].Create()
	for i := 1; i < N1; i++ {
		nodes[i].Join(makeLocalAddr(i / 2))
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < N; i++ {
		nodes[i%N1].Put(fmt.Sprint(i), fmt.Sprint(i))
	}
	time.Sleep(200 * time.Millisecond)
	for i := N1; i < N; i++ {
		nodes[i].Join(makeLocalAddr(i / 2))
		time.Sleep(400 * time.Millisecond)
	}
	for i := 0; i < N; i++ {
		_, val := nodes[i].Get(fmt.Sprint(i))
		fmt.Printf("%d : %s\n", i, val)
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < N; i++ {
		nodes[i].Quit()
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}

func TestSmallForceQuit(t *testing.T) {
	const N, M = 5, 20
	var FQ = [...]int32{2, 3, 4}
	var nodes [N]*ChordNode
	for i := 0; i < N; i++ {
		nodes[i] = CreateChordNode(makeLocalAddr(i))
		nodes[i].Run()
	}
	time.Sleep(200 * time.Millisecond)
	nodes[0].Create()
	for i := 1; i < N; i++ {
		nodes[i].Join(makeLocalAddr(i / 2))
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < M; i++ {
		nodes[i%N].Put(fmt.Sprint(i), fmt.Sprint(i))
	}
	time.Sleep(200 * time.Millisecond)
	for _, v := range FQ {
		nodes[v].ForceQuit()
		time.Sleep(400 * time.Millisecond)
	}
	for i := 0; i < M; i++ {
		ok, val := nodes[0].Get(fmt.Sprint(i))
		if ok {
			fmt.Printf("%d : %s\n", i, val)
		} else {
			fmt.Printf("%d failed\n", i)
		}
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < N; i++ {
		nodes[i].Quit()
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}

func TestQAFQ(t *testing.T) {
	const N, M = 10, 40
	var FQ = [...]int32{3, 4, 9}
	var Q = [...]int32{5, 6, 7}
	var nodes [N]*ChordNode
	for i := 0; i < N; i++ {
		nodes[i] = CreateChordNode(makeLocalAddr(i))
		nodes[i].Run()
	}
	time.Sleep(200 * time.Millisecond)
	nodes[0].Create()
	for i := 1; i < N; i++ {
		nodes[i].Join(makeLocalAddr(i / 2))
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("start putting")
	for i := 0; i < M; i++ {
		nodes[i%N].Put(fmt.Sprint(i), fmt.Sprint(i))
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Println("start quitting")
	for _, v := range Q {
		nodes[v].Quit()
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < M; i++ {
		ok, val := nodes[i%3].Get(fmt.Sprint(i))
		if ok {
			fmt.Printf("%d : %s\n", i, val)
		} else {
			fmt.Printf("%d failed\n", i)
		}
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Println("start force quitting")
	for _, v := range FQ {
		nodes[v].ForceQuit()
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < M; i++ {
		ok, val := nodes[i%3].Get(fmt.Sprint(i))
		if ok {
			fmt.Printf("%d : %s\n", i, val)
		} else {
			fmt.Printf("%d failed\n", i)
		}
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < N; i++ {
		nodes[i].Quit()
		time.Sleep(400 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}
