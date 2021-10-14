package chord

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

func checkForError(err error) {
	if err != nil {
		fmt.Println("Error Encountered:", err)
	}
}

func saveNode(db *sql.DB, ip, successor string) {
	save := "INSERT INTO chord(self, successor) VALUES(?,?)"
	stmt, err := db.Prepare(save)
	checkForError(err)

	stmt.Exec(ip, successor)

	stmt.Close()
}

func updateSuccessor(db *sql.DB, self, successor string) {
	upd := "UPDATE chord SET successor=? WHERE self=?"

	stmt, err := db.Prepare(upd)
	checkForError(err)

	stmt.Exec(successor, self)
	stmt.Close()
}

func deleteNode(db *sql.DB, self string, wg *sync.WaitGroup) {
	defer wg.Done()
	del := "DELETE FROM chord WHERE self=?"

	stmt, err := db.Prepare(del)
	checkForError(err)

	stmt.Exec(self)
	stmt.Close()
}
