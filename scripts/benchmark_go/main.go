package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	nats "github.com/nats-io/nats.go"
)

func main() {
	if len(os.Args) != 5 {
		panic("usage: benchmark <mode> <url> <count> <payload-size>")
	}
	mode := os.Args[1]
	url := os.Args[2]
	count, _ := strconv.Atoi(os.Args[3])
	size, _ := strconv.Atoi(os.Args[4])
	nc, err := nats.Connect(url, nats.Name("enats-performance-comparison"))
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	switch mode {
	case "pub":
		runPub(nc, count, size, false)
	case "pubsync":
		runPub(nc, count, size, true)
	case "request":
		runRequest(nc, count, size)
	case "pubsub":
		runPubSub(nc, count, size, false)
	case "pubsubsync":
		runPubSub(nc, count, size, true)
	case "jetstream", "jetstream_msgid":
		runJetStream(nc, count, size, mode == "jetstream_msgid")
	default:
		panic("unknown mode")
	}
}

func runPub(nc *nats.Conn, count, size int, syncEach bool) {
	payload := bytes.Repeat([]byte("x"), size)
	started := time.Now()
	for i := 0; i < count; i++ {
		if err := nc.Publish("benchmark.events", payload); err != nil {
			panic(err)
		}
		if syncEach {
			if err := nc.Flush(); err != nil {
				panic(err)
			}
		}
	}
	if !syncEach {
		if err := nc.Flush(); err != nil {
			panic(err)
		}
	}
	mode := "pub"
	if syncEach {
		mode = "pubsync"
	}
	report(mode, count, time.Since(started), nil)
}

func runRequest(nc *nats.Conn, count, size int) {
	payload := bytes.Repeat([]byte("x"), size)
	if _, err := nc.Subscribe("benchmark.request", func(msg *nats.Msg) {
		if err := nc.Publish(msg.Reply, msg.Data); err != nil {
			panic(err)
		}
	}); err != nil {
		panic(err)
	}
	if err := nc.Flush(); err != nil {
		panic(err)
	}
	latencies := make([]int64, 0, count)
	started := time.Now()
	for i := 0; i < count; i++ {
		t := time.Now()
		if _, err := nc.Request("benchmark.request", payload, 5*time.Second); err != nil {
			panic(err)
		}
		latencies = append(latencies, time.Since(t).Microseconds())
	}
	report("request", count, time.Since(started), latencies)
}

func runPubSub(nc *nats.Conn, count, size int, syncEach bool) {
	sub, err := nc.SubscribeSync("benchmark.pubsub")
	if err != nil {
		panic(err)
	}
	if err := nc.Flush(); err != nil {
		panic(err)
	}
	payload := make([]byte, max(size, 8))
	latencies := make([]int64, 0, count)
	started := time.Now()
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
		if err := nc.Publish("benchmark.pubsub", payload); err != nil {
			panic(err)
		}
		if syncEach {
			if err := nc.Flush(); err != nil {
				panic(err)
			}
			msg, err := sub.NextMsg(5 * time.Second)
			if err != nil {
				panic(err)
			}
			latencies = append(latencies, (time.Now().UnixNano()-int64(binary.BigEndian.Uint64(msg.Data[:8])))/1000)
		}
	}
	if !syncEach {
		if err := nc.Flush(); err != nil {
			panic(err)
		}
		for i := 0; i < count; i++ {
			msg, err := sub.NextMsg(5 * time.Second)
			if err != nil {
				panic(err)
			}
			latencies = append(latencies, (time.Now().UnixNano()-int64(binary.BigEndian.Uint64(msg.Data[:8])))/1000)
		}
	}
	report(map[bool]string{false: "pubsub", true: "pubsubsync"}[syncEach], count, time.Since(started), latencies)
}

func runJetStream(nc *nats.Conn, count, size int, withMsgID bool) {
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	subject, stream := "benchmark.js."+suffix, "BENCH_"+suffix
	if _, err := js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}}); err != nil {
		panic(err)
	}
	payload := bytes.Repeat([]byte("x"), size)
	if _, err := js.Publish(subject, payload); err != nil {
		panic(err)
	}
	latencies := make([]int64, 0, count)
	started := time.Now()
	for i := 0; i < count; i++ {
		started := time.Now()
		var opts []nats.PubOpt
		if withMsgID {
			opts = append(opts, nats.MsgId(strconv.Itoa(i)))
		}
		if _, err := js.Publish(subject, payload, opts...); err != nil {
			panic(err)
		}
		latencies = append(latencies, time.Since(started).Microseconds())
	}
	report(map[bool]string{false: "jetstream", true: "jetstream_msgid"}[withMsgID], count, time.Since(started), latencies)
}

func report(mode string, count int, duration time.Duration, latencies []int64) {
	if len(latencies) == 0 {
		fmt.Printf("client=go mode=%s count=%d duration_us=%d throughput_msg_s=%.2f\n", mode, count, duration.Microseconds(), float64(count)*1e6/float64(max64(duration.Microseconds(), 1)))
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("client=go mode=%s count=%d throughput_msg_s=%.2f p50_us=%d p90_us=%d p95_us=%d p99_us=%d\n", mode, count, float64(count)*1e6/float64(max64(duration.Microseconds(), 1)), percentile(latencies, 50), percentile(latencies, 90), percentile(latencies, 95), percentile(latencies, 99))
}

func percentile(values []int64, p int) int64 {
	index := (len(values)*p + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
