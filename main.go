package engineerdb

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type Engineer struct {
	ID        int
	FirstName string
	LastName  string
	Gender    int
	CountryID int
	Country   Country
	Title     string
	Created   time.Time
}

type Country struct {
	ID          int
	CountryName string
	Created     time.Time
}

var (
	Hostname = ""
	Port     = 5432
	Username = ""
	Password = ""
	Database = ""
)

func openConn() (*sql.DB, error) {
	conn, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database))
	if err != nil {
		return nil, err
	}
	return conn, err
}

func AddEngineer(e Engineer) (int, error) {
	db, err := openConn()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	result, err := db.Exec(`INSERT INTO engineer(first_name, last_name, gender, country_id, title) VALUES($1, $2, $3, $4, $5) RETURNING id`,
		e.FirstName, e.LastName, e.Gender, e.ID, e.Title,
	)
	if err != nil {
		return 0, err
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		fmt.Println("Cannot get the inserted id")
	}
	return int(insertedId), nil
}

func UpdateEngineer(e Engineer) error {
	db, err := openConn()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`
		UPDATE engineer 
		SET first_name = $1, last_name = $2, gender = $3, country_id = $4, title = $5 
		WHERE id = $6`,
		e.FirstName, e.LastName, e.Gender, e.ID, e.Title, e.ID,
	)
	if err != nil {
		return err
	}

	return nil
}

func exist(id int) (bool, error) {
	db, err := openConn()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id FROM engineer WHERE id = $1`,
		id,
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	existId := -1
	for rows.Next() {
		if err := rows.Scan(&existId); err != nil {
			return false, err
		}
	}

	return existId > 0, nil
}

func List() ([]Engineer, error) {
	db, err := openConn()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	engineers := []Engineer{}
	rows, err := db.Query(`
		SELECT e.*, c.country_name FROM engineer e
		JOIN country c ON e.country_id = c.id
		LIMIT 2000
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		engineer := Engineer{}
		if err := rows.Scan(
			&engineer.ID, &engineer.FirstName, &engineer.LastName, &engineer.Gender,
			&engineer.CountryID, &engineer.Title, &engineer.Created, &engineer.Country.CountryName,
		); err != nil {
			return nil, err
		}
		engineers = append(engineers, engineer)
	}

	return engineers, nil
}

func DeleteEngineer(id int) error {
	db, err := openConn()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`DELETE FROM engineer WHERE id = $1`, id)
	if err != nil {
		return err
	}

	return nil
}
