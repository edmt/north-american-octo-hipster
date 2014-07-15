package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/edmt/north_american_octo_hipster/db"
	"io/ioutil"
	"net/http"
	"os"
)

type RGResponseHandler struct {
	Connection *sql.DB
}

func (h RGResponseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	records := db.Query(h.Connection, string(body))
	rawData, _ := json.Marshal(records)
	response := string(rawData) + "\n"
	fmt.Fprint(w, response)
}

func main() {
	connectionParamaters := db.ConnectionParameters{
		Host:     os.Getenv("RGHOST"),
		Port:     os.Getenv("RGPORT"),
		User:     os.Getenv("RGSQLUSER"),
		Password: os.Getenv("RGSQLPASSWORD"),
		Database: os.Getenv("RGDATABASE"),
	}
	conn := connectionParamaters.MakeConnection()
	defer conn.Close()
	db.Ping(conn)

	var rh RGResponseHandler
	rh.Connection = conn
	http.ListenAndServe("localhost:4000", rh)
}
