package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	mysqlManager, err := NewMySQLManager()

	if err != nil {
		log.Fatalf("Failed to initialize MySQL manager: %v", err)
	}

	defer mysqlManager.db.Close()

	err = mysqlManager.CreateTables()

	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	TcpClient(mysqlManager)

	interrupt := make(chan os.Signal, 1)

	<-interrupt
}

func NewMySQLManager() (*mySqlManager, error) {
	db, err := sql.Open("mysql", "root:iW9rH4fU7lhP@/SCPSL")

	if err != nil {
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		return nil, err
	}

	return &mySqlManager{db}, nil
}

func TcpClient(mysqlManager *mySqlManager) {
	port, err := ReadConfig("port")

	if err != nil {
		log.Fatalf("Failed to read port from config: %v", err)
	}

	conn, err := net.Dial("tcp", "localhost:"+port)

	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	defer conn.Close()

	for {

		message, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			log.Println("Failed to read message from server:", err)
			break
		}

		message = strings.TrimRight(message, "\n")
		parts := strings.Split(message, " ")
		methodName := parts[0]
		args := parts[1:]

		switch methodName {
		case "Insert":
			stats := Parse(args[0])
			err := mysqlManager.Insert(stats)

			if err != nil {
				log.Println("Failed to insert statistics:", err)
			}
		case "Check":
			userID := args[0]
			err := mysqlManager.Check(userID)

			if err != nil {
				log.Println("Failed to check statistics:", err)
			}
		case "Update":

			stats := Parse(args[1])
			err := mysqlManager.Update(args[0], stats)

			if err != nil {
				log.Println("Failed to update statistics:", err)
			}

		case "AddIpAddress":

			err := mysqlManager.AddIpAddress(args[0], args[1])

			if err != nil {
				log.Println("Failed to add ip addresses:", err)
			}

		case "AddDeaths":

			deaths, _ := strconv.Atoi(args[1])
			err := mysqlManager.AddDeaths(args[0], deaths)

			if err != nil {
				log.Println("Failed to add deaths:", err)
			}

		case "AddScpKills":

			scpKills, _ := strconv.Atoi(args[1])
			err := mysqlManager.AddScpKills(args[0], scpKills)

			if err != nil {
				log.Println("Failed to add scp kills:", err)
			}

		case "AddHumanKills":

			humanKills, _ := strconv.Atoi(args[1])
			err := mysqlManager.AddHumanKills(args[0], humanKills)

			if err != nil {
				log.Println("Failed to add human kills:", err)
			}

		case "AddPlayTime":

			playTime, _ := strconv.ParseInt(args[1], 10, 64)
			err := mysqlManager.AddPlayTime(args[0], playTime)

			if err != nil {
				log.Println("Failed to add play time:", err)
			}

		case "SetLastPlayed":

			lastPlayed, _ := strconv.ParseInt(args[1], 10, 64)
			err := mysqlManager.SetLastPlayed(args[0], lastPlayed)

			if err != nil {
				log.Println("Failed to add last played time:", err)
			}

		default:
			log.Println("Invalid method name:", methodName)
		}
	}
}

func ReadConfig(key string) (string, error) {

	file, err := os.Open("config.cfg")

	if err != nil {
		return "", err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "=")
		if parts[0] == key {
			return parts[1], nil
		}
	}

	return "", fmt.Errorf("Key %s not found in config file", key)
}

type mySqlManager struct {
	db *sql.DB
}

func (m *mySqlManager) Insert(stats Statistics) error {

	_, err := m.db.Exec("INSERT INTO PlayerStats (userId, ipAddresses, deaths, scpKills, humanKills, playTime, lastPlayed) VALUES (?, ?, ?, ?, ?, ?, ?)",
		stats.UserId, stats.IpAddresses, stats.Deaths, stats.ScpKills, stats.HumanKills, stats.PlayTime, stats.LastPlayed)

	return err
}

func (m *mySqlManager) Check(userID string) error {

	isExists, err := m.Exists(userID)

	if err != nil {
		return err
	}

	if isExists {
		return nil
	}

	stats := Statistics{
		UserId:      userID,
		IpAddresses: []string{},
		Deaths:      0,
		ScpKills:    0,
		HumanKills:  0,
		PlayTime:    0,
		LastPlayed:  0,
	}

	m.Insert(stats)

	return nil
}

func (m *mySqlManager) Update(userID string, stats Statistics) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET ipAddresses = ?, deaths = ?, scpKills = ?, humanKills = ?, playTime = ?, lastPlayed = ? WHERE userId = ?", stats.IpAddresses, stats.Deaths, stats.ScpKills, stats.HumanKills, stats.PlayTime, stats.LastPlayed, userID)

	return err
}

func (m *mySqlManager) AddIpAddress(userID string, ipAddress string) error {

	stats, err := m.Get(userID)

	if err != nil {
		return err
	}

	_, err = m.db.Exec("UPDATE PlayerStats SET ipAddresses = ipAddresses || ? WHERE userId = ?", append(stats.IpAddresses, ipAddress), userID)

	return err
}

func (m *mySqlManager) AddDeaths(userID string, deaths int) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET deaths = deaths + ? WHERE userId = ?", deaths, userID)

	return err
}

func (m *mySqlManager) AddScpKills(userID string, scpKills int) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET scpKills = scpKills + ? WHERE userId = ?", scpKills, userID)

	return err
}

func (m *mySqlManager) AddHumanKills(userID string, humanKills int) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET humanKills = humanKills + ? WHERE userId = ?", humanKills, userID)

	return err
}

func (m *mySqlManager) AddPlayTime(userID string, playTime int64) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET playTime = playTime + ? WHERE userId = ?", playTime, userID)

	return err
}

func (m *mySqlManager) SetLastPlayed(userID string, lastPlayed int64) error {

	_, err := m.db.Exec("UPDATE PlayerStats SET lastPlayed = ? WHERE userId = ?", lastPlayed, userID)

	return err
}

func (m *mySqlManager) Get(userID string) (Statistics, error) {

	var stats Statistics
	err := m.db.QueryRow("SELECT userId, ipAddresses, deaths, scpKills, humanKills, playTime, lastPlayed FROM PlayerStats WHERE userId = ?", userID).Scan(&stats.UserId, &stats.IpAddresses, &stats.Deaths, &stats.ScpKills, &stats.HumanKills, &stats.PlayTime, &stats.LastPlayed)

	if err != nil {
		return Statistics{}, err
	}

	return stats, nil
}

func (m *mySqlManager) Exists(userID string) (bool, error) {
	var exists bool
	err := m.db.QueryRow("SELECT 1 FROM PlayerStats WHERE userId = ?", userID).Scan(&exists)

	if err != nil {
		return false, err
	}

	return exists, nil
}

func (m *mySqlManager) CreateTables() error {
	_, err := m.db.Exec(`CREATE TABLE IF NOT EXISTS PlayerStats (
					userId VARCHAR(256) PRIMARY KEY,
					ipAdresses TEXT NOT NULL,
					deaths INTEGER NOT NULL,
					scpKills INTEGER NOT NULL,
					humanKills INTEGER NOT NULL,
					playTime INTEGER NOT NULL,
					lastPlayed INTEGER NOT NULL
				)`)
	return err
}

type Statistics struct {
	UserId      string
	IpAddresses []string
	Deaths      int
	ScpKills    int
	HumanKills  int
	PlayTime    time.Duration
	LastPlayed  time.Duration
}

func (s *Statistics) KillDeathRatio() float64 {
	return float64(s.ScpKills+s.HumanKills) / float64(s.Deaths)
}

func Parse(input string) Statistics {
	parts := strings.Split(input, ",")

	userId := parts[0]
	ipAddresses := strings.Split(parts[1], ";")
	deaths, _ := strconv.Atoi(parts[2])
	scpKills, _ := strconv.Atoi(parts[3])
	humanKills, _ := strconv.Atoi(parts[4])
	playTime, _ := time.ParseDuration(parts[5] + "s")
	lastPlayed, _ := time.ParseDuration(parts[6] + "s")

	return Statistics{userId, ipAddresses, deaths, scpKills, humanKills, playTime, lastPlayed}
}
