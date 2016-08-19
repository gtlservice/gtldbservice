package main

import (
	"database/sql"
	"errors"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlStateOpened = 1
	mysqlStateClosed = 2
)

type mysqlInstance struct {
	state int
	url   string
	conn  *sql.DB
}

func NewMysql() *mysqlInstance {
	inst := new(mysqlInstance)
	inst.state = mysqlStateClosed
	return inst
}

func (m *mysqlInstance) openMysqlConnection(url string) error {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return errors.New("open mysql failed ")
	}
	err = db.Ping()
	if err != nil {
		return errors.New("ping mysql failed")
	}
	m.conn = db
	return nil
}

func (m *mysqlInstance) closeMysqlConnection() {
	m.conn.Close()
}

func (m *mysqlInstance) execInsert(sqlstr string) (int, error) {
	Ret, err := m.conn.Exec(sqlstr)
	if err != nil {
		log.Println("insert data failed")
		return 0, errors.New("insert failed")
	}
	affected, err := Ret.RowsAffected()
	if err != nil {
		return 0, errors.New("insert data failed")
	}
	return int(affected), nil
}

func (m *mysqlInstance) execRead(sqlstr string) (interface{}, error) {
	row := m.conn.QueryRow(sqlstr)
	var datarow interface{}
	err := row.Scan(datarow)
	if err != nil {
		return nil, errors.New("read failed")
	}
	return datarow, nil
}

func (m *mysqlInstance) execUpdate(sqlstr string) (int, error) {
	ret, err := m.conn.Exec(sqlstr)
	if err != nil {
		return 0, errors.New("update failed")
	}
	aff, err := ret.RowsAffected()
	if err != nil {
		return 0, errors.New("update failed2")
	}
	return int(aff), nil
}

func (m *mysqlInstance) execDelete(sqlstr string) (int, error) {
	ret, err := m.conn.Exec(sqlstr)
	if err != nil {
		return 0, errors.New("delete failed")
	}
	aff, err := ret.RowsAffected()
	if err != nil {
		return 0, errors.New("delete failed2")
	}
	return int(aff), nil
}
