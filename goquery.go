package main

import (
	"fmt"
	"log"
	"os"

	// Importa los drivers para la conexión con la base de datos
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"

	// "reflect"
	"encoding/json"
)

// Construye la cadena conexión basada en variables de entorno
func makeConnectionString() string {
	host := os.Getenv("HOST")
	port := "1433"
	user := os.Getenv("SQLUSER")
	password := os.Getenv("SQLPASSWORD")
	database := os.Getenv("DATABASE")
	return fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s;log=2",
		host, port, user, password, database)
}

// Revisa que pueda acceder a la base de datos
func ping(db *sql.DB) {
	err := db.Ping()
	if err == nil {
		log.Print("Everything is ok")
	} else {
		log.Panic(err)
	}
}

// Realiza una consulta a la base de datos y recupera un conjunto de registros
func queryResultSet(db *sql.DB) {
	// Asigna un registro a la vez a variables con el tipo de dato apropiado
	var (
		rfc         string
		razonSocial string
	)
	rows, err := db.Query("select rfc, razonSocial from empresas where rfc = ?",
		"AAA010101AAA")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&rfc, &razonSocial)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(rfc, razonSocial)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

// Realiza una consulta a la base de datos y recupera un conjunto de registros
func queryJSON(db *sql.DB) {
	rows, err := db.Query("select rfc, razonSocial from empresas where rfc = ?",
		"AAA010101AAA")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
 	scanArgs := make([]interface{}, len(columns))
    values   := make([]interface{}, len(columns))

    for i := range values { scanArgs[i] = &values[i] }

	for rows.Next() {
        err = rows.Scan(scanArgs...)
        record := make(map[string]interface{})

        for i, col := range values {        
        	record[columns[i]] = col
        	//fmt.Printf("\n%s: type= %s %s\n", columns[i], reflect.TypeOf(col), col)
        }
        log.Print(record)
        data, _ := json.Marshal(record)
        log.Print(string(data))
    }

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

// Realiza una consulta mediante una sentencia preparada
func queryPreparedResultSet(db *sql.DB) {
	var (
		rfc         string
		razonSocial sql.NullString
	)
	stmt, err := db.Prepare("select rfc, razonSocial from empresas where rfc = ?")

	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query("AAA010101AAA")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&rfc, &razonSocial)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(rfc, razonSocial.String, razonSocial.Valid)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

// Realiza una consulta de un sólo registro
func querySingleRow(db *sql.DB) {
	var total int
	err := db.QueryRow("select count(*) total from empresas").Scan(&total)
	if err != nil {
		log.Fatal(err)
	}
	log.Print(total)
}

// Ejecuta una sentencia de actualización de datos (insert, update, delete)
func execStatement(db *sql.DB) {
	stmt, err := db.Prepare("INSERT INTO empresas VALUES(?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	result, err := stmt.Exec("Urielón Lennon", "URINANDO SA de CV")
	if err != nil {
		log.Fatal(err)
	}
	lastId, err := result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	rowCount, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ID = %d, affected = %d\n", lastId, rowCount)
}

func main() {
	db, err := sql.Open("mssql", makeConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping a la base de datos
	ping(db)

	// Lectura
	queryResultSet(db)
	queryPreparedResultSet(db)
	querySingleRow(db)
	queryJSON(db)

	// Modificación
	// execStatement(db)
}
