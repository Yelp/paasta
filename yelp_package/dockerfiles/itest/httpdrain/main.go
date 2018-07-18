package main

import "net/http"
import "fmt"
import "io/ioutil"
import "net/http/httputil"
import "os"
import "time"
import "strconv"

const drainFile = "drain"

func drain(w http.ResponseWriter, r *http.Request) {
	var _, err = os.Stat(drainFile)
	if os.IsNotExist(err) {
		var file, err = os.Create(drainFile)
		if err != nil {
			w.WriteHeader(500)
			fmt.Printf("error creating drainFile %v", err)
			return
		}
		defer file.Close()
		_, err = file.WriteString(fmt.Sprint(time.Now().Unix()))
		if err != nil {
			w.WriteHeader(500)
			fmt.Printf("error writing to drain file %v", err)
			return
		}
	}
}

func stopDraining(w http.ResponseWriter, r *http.Request) {
	var _, err = os.Stat(drainFile)
	if os.IsNotExist(err) {
		w.WriteHeader(200)
		return
	}
	err = os.Remove(drainFile)
	if err != nil {
		fmt.Printf("error deleting file")
		w.WriteHeader(500)
	} else {
		w.WriteHeader(200)
	}
}

func drainStatus(w http.ResponseWriter, r *http.Request) {
	var _, err = os.Stat(drainFile)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
		}
	} else {
		w.WriteHeader(200)
	}
}

func isSafeToKill(w http.ResponseWriter, r *http.Request) {
	var _, err = os.Stat(drainFile)
	if os.IsNotExist(err) {
		fmt.Printf("file does not exist\n")
		w.WriteHeader(400)
		return
	}
	if err != nil {
		fmt.Printf("err stat'ing file\n")
		w.WriteHeader(500)
		return
	}
	dat, err := ioutil.ReadFile(drainFile)
	if err != nil {
		fmt.Printf("err reading contents of file\n")
		w.WriteHeader(500)
		return
	}
	i, err := strconv.ParseInt(string(dat), 10, 64)
	tm := time.Unix(i, 0)
	duration := time.Since(tm)
	if duration.Seconds() > 2 {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(400)
	}
}

func log(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dump, _ := httputil.DumpRequest(r, true)
		fmt.Printf("%q\n", dump)
		h(w, r)
	})
}

func main() {
	http.HandleFunc("/drain", log(drain))
	http.HandleFunc("/drain/stop", log(stopDraining))
	http.HandleFunc("/drain/status", log(drainStatus))
	http.HandleFunc("/drain/safe_to_kill", log(isSafeToKill))
	err := http.ListenAndServe(":3000", nil)
	if err != nil {
		fmt.Printf("%v", err)
	}
}
