package db

import (
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
)

type ConnectionParameters struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// Construye la cadena conexión
func (cp ConnectionParameters) makeConnectionString() string {
	return fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s;log=2",
		cp.Host, cp.Port, cp.User, cp.Password, cp.Database)
}

func (cp ConnectionParameters) MakeConnection() *sql.DB {
	stringConnection := cp.makeConnectionString()
	log.Print(stringConnection)
	db, err := sql.Open("mssql", stringConnection)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// Revisa que pueda acceder a la base de datos
func Ping(db *sql.DB) {
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
func Query(db *sql.DB, query string) []map[string]interface{} {
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var records []map[string]interface{}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		record := make(map[string]interface{})

		for i, col := range values {
			record[columns[i]] = col
			//fmt.Printf("\n%s: type= %s %s\n", columns[i], reflect.TypeOf(col), col)
		}
		// data, _ := json.Marshal(record)
		// log.Print(string(data))
		records = append(records, record)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return records
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
