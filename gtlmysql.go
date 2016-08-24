package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlStateOpened = 1
	mysqlStateClosed = 2
)

type record struct {
	line map[string]interface{}
}

type MysqlInstance struct {
	state int
	url   string
	conn  *sql.DB
	rows  *sql.Rows
	data  []record
}

//NewMysql ...
func NewMysql() *MysqlInstance {
	inst := new(MysqlInstance)
	inst.state = mysqlStateClosed
	return inst
}

func (m *MysqlInstance) openMysqlConnection(url string) error {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return errors.New("open mysql failed ")
	}
	err = db.Ping()
	if err != nil {
		return errors.New("ping mysql failed")
	}
	m.conn = db
	m.state = mysqlStateOpened
	return nil
}

func (m *MysqlInstance) closeMysqlConnection() {
	m.conn.Close()
	m.state = mysqlStateClosed
}

func (m *MysqlInstance) execMysqlInsert(sqlstr string) (int, error) {
	if m.state == mysqlStateClosed {
		return 0, errors.New("connections is closed")
	}

	Ret, err := m.conn.Exec(sqlstr)
	if err != nil {
		log.Println("insert data failed", err)
		return 0, errors.New("insert failed")
	}
	affected, err := Ret.RowsAffected()
	if err != nil {
		return 0, errors.New("insert data failed")
	}
	log.Println("affected rows ", affected)
	return int(affected), nil
}

func (m *MysqlInstance) execMysqlRead(sqlstr string) (*sql.Rows, error) {
	if m.state == mysqlStateClosed {
		return nil, errors.New("connections is closed")
	}
	rows, err := m.conn.Query(sqlstr)
	if err != nil {
		return nil, errors.New("read mysql failed")
	}
	return rows, nil
}

func (m *MysqlInstance) execMysqlUpdate(sqlstr string) (int, error) {
	if m.state == mysqlStateClosed {
		return 0, errors.New("connections is closed")
	}
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

func (m *MysqlInstance) execMysqlDelete(sqlstr string) (int, error) {
	if m.state == mysqlStateClosed {
		return 0, errors.New("connections is closed")
	}
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

//return affected lines or error
func (m *MysqlInstance) writeUserInfo(acc_name, password, secureQuestion, secureAnswer, email, phoneNumber string) (int, error) {
	if m.state == mysqlStateClosed {
		return 0, errors.New("connections is closed")
	}
	sql := "insert into user_info(acc_name, password, secure_question, secure_answer, email, phone_number) values ('%s', '%s', '%s', '%s', '%s', '%s')"
	sql2 := fmt.Sprintf(sql, acc_name, password, secureQuestion, secureAnswer, email, phoneNumber)
	affect, err := m.execMysqlInsert(sql2)
	if err != nil {
		return 0, err
	}
	return affect, nil
}

func (m *MysqlInstance) readUserInfo(key, keyValue string) (int, []record, error) {
	if m.state == mysqlStateClosed {
		return 0, nil, errors.New("connections is closed")
	}
	sql := "select * from  user_info where '%s' = '%s'"
	sql2 := fmt.Sprintf(sql, key, keyValue)
	rows, err := m.execMysqlRead(sql2)
	if err != nil {
		return 0, nil, err
	}
	m.rows = rows
	m.data = make([]record, 1)
	var id, cnt int
	var accName, password, secure_question, secure_answer, email, phone_number string
	for m.rows.Next() {
		var aRecord record
		aRecord.line = map[string]interface{}{}
		rows.Scan(&id, &accName, &password, &secure_question, &secure_answer, &email, &phone_number)
		aRecord.line["id"] = id
		aRecord.line["acc_name"] = accName
		aRecord.line["password"] = password
		aRecord.line["secure_question"] = secure_question
		aRecord.line["secure_answer"] = secure_answer
		aRecord.line["email"] = email
		aRecord.line["phone_number"] = phone_number
		m.data = append(m.data, aRecord)
		cnt++
	}
	return cnt, m.data, nil
}
