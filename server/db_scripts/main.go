package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func main() {
	dsns := []string{
		"root@tcp(localhost:3306)/lab3_1?parseTime=true",
		"root@tcp(localhost:3306)/lab3_2?parseTime=true",
		"root@tcp(localhost:3306)/lab3_3?parseTime=true",
		"root@tcp(localhost:3306)/lab3_4?parseTime=true",
		"root@tcp(localhost:3306)/lab3_5?parseTime=true",
		"root@tcp(localhost:3306)/lab3_6?parseTime=true",
		"root@tcp(localhost:3306)/lab3_7?parseTime=true",
		"root@tcp(localhost:3306)/lab3_8?parseTime=true",
		"root@tcp(localhost:3306)/lab3_9?parseTime=true",
	}

	for _, dsn := range dsns {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("MySQL Connected!!")

		query := `delete from transaction`
		_, err = db.Exec(query)
		if err != nil {
			fmt.Println("Error deleting:", err)
		}

		for i := 1; i <= 3000; i++ {
			query := `update user set balance = ? where user = ?`
			_, err := db.Exec(query, 10, i)
			if err != nil {
				fmt.Println("Error inserting row:", err)
			}
		}
	}
}
