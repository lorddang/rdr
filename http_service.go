package main

import (
	"encoding/json"
	"fmt"
	"github.com/dongmx/rdb"
	"net/http"
	"os"
)

var taskChannel = make(chan task, 10)

type task struct {
	namespace string
	filename  string
	port      string
	action    string
}

func consumerTask() {
	for {
		select {
		case t := <-taskChannel:
			go parseFile(t.filename, t.port, t.namespace)
		}
	}

}

func submitTask(t task) {
	taskChannel <- t
}

func newTask(filename, port, namespace string) task {
	return task{
		namespace: namespace,
		filename:  filename,
		port:      port,
	}
}

func parse(respw http.ResponseWriter, req *http.Request) {
	result := map[string]interface{}{}
	req.ParseForm()
	var namespace string
	if len(req.Form["namespace"]) > 0 {
		namespace = req.Form["namespace"][0]
	}

	var file string
	if len(req.Form["filepath"]) > 0 {
		file = req.Form["filepath"][0]
	}
	var port string
	if len(req.Form["port"]) > 0 {
		port = req.Form["port"][0]
	}
	var data []byte
	if isAbsPath(file) && isExsist(file) {
		if isVaildPort(port) {
			var task = newTask(file, port, namespace)
			submitTask(task)
			result["ret"] = true
			result["message"] = "submit ok"
			result["code"] = 0
			data, _ = json.Marshal(result)
		} else {
			result["ret"] = false
			result["message"] = "端口错误"
			result["code"] = 2
			data, _ = json.Marshal(result)

		}
	} else {
		result["ret"] = false
		result["message"] = "文件不存在或不是绝对路径"
		result["code"] = 1
		data, _ = json.Marshal(result)

	}
	respw.Write([]byte(data))

}

func serviceDecode(decoder *Decoder, filepath string) {
	f, err := os.Open(filepath)
	if err != nil {
		fmt.Fprintf(os.Stdout, "open rdbfile err: %v\n", err)
		close(decoder.Entries)
		return
	}
	err = rdb.Decode(f, decoder)
	if err != nil {
		fmt.Fprintf(os.Stdout, "decode rdbfile err: %v\n", err)
		close(decoder.Entries)
		return
	}
}

func parseFile(file, port, namespace string) {
	decoder := NewDecoder()
	go serviceDecode(decoder, file)
	counter := NewCounter()
	counter.Count(decoder.Entries)
	counter.persist(namespace + ":" + port)
}
