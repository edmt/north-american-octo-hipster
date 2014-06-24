package main

import (
    "fmt"
    "net/http"
    "io/ioutil"
)

type Hello struct{}

func (h Hello) ServeHTTP(w http.ResponseWriter, r *http.Request) {	
	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	response := string(body) + "\n"
    fmt.Fprint(w, response)
}

func main() {
    var h Hello
    http.ListenAndServe("localhost:4000", h)
}

// $ curl localhost:4000 -XGET -H 'content-type: application/json' -d '{"hello": true}'