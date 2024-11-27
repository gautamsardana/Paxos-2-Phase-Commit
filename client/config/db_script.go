package config

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func PopulateDB(dsn string, server, userStart, userEnd int32) {
	dsn = fmt.Sprintf(dsn, server)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	query := `delete from transaction`
	_, err = db.Exec(query)
	if err != nil {
		fmt.Println("Error deleting:", err)
	}

	for i := userStart; i <= userEnd; i++ {
		query := `update user set balance = ? where user = ?`
		_, err := db.Exec(query, 10, i)
		if err != nil {
			fmt.Println("Error inserting row:", err)
		}
	}
	fmt.Printf("Done for db:%s\n", dsn)
}
