package storage

// leader handles all "mutable" requests, while followers handle read requests only, and forward "mutable" requests
// to leader
// TODO to make it scalable :
// 1. storage hierarchy should be splitted to keep each part small
// 2. don't always rely on leader to assign task to mesos

import (
	"fmt"
	"github.com/more-free/mesos_scheduler/protocol"
	zkCli "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Storage interface {
	Get(data *protocol.Get) ([]byte, error)
	GetAll() ([]string, error)
	Update(data *protocol.Update) error
	Create(data *protocol.Post) (string, error) // returns unique task id
	Delete(data *protocol.Delete) error
	DeleteAll() error
	Open() error
	Close() error
}

type ZkStorage struct {
	hostports []string
	rootDir   string
	timeout   time.Duration
	flags     int32
	acl       []zkCli.ACL
	conn      *zkCli.Conn
}

func NewZkStorage(servers []string, rootDir string) Storage {
	rand.Seed(time.Now().UTC().UnixNano())

	return &ZkStorage{
		hostports: servers,
		rootDir:   rootDir,
		timeout:   time.Second * 3,
		flags:     int32(0), // persistent node
		acl:       zkCli.WorldACL(zkCli.PermAll),
	}
}

func (zk *ZkStorage) Open() error {
	if !strings.HasPrefix(zk.rootDir, "/") {
		return fmt.Errorf("root dir must start with '/'")
	}
	if strings.HasSuffix(zk.rootDir, "/") {
		zk.rootDir = zk.rootDir[:len(zk.rootDir)-1]
	}

	conn, _, err := zkCli.Connect(zk.hostports, zk.timeout)
	if err != nil {
		return err
	} else {
		exists, _, err := conn.Exists(zk.rootDir)
		if err != nil {
			return err
		}

		if !exists {
			err = zk.createDir(conn, zk.rootDir)
			if err != nil {
				conn.Close()
				return err
			}
		}

		zk.conn = conn
		return nil
	}
}

func (zk *ZkStorage) Close() error {
	zk.conn.Close()
	return nil
}

func (zk *ZkStorage) Get(data *protocol.Get) ([]byte, error) {
	bytes, _, err := zk.conn.Get(zk.getPath(data.TaskId))
	return bytes, err
}

func (zk *ZkStorage) GetAll() ([]string, error) {
	children, _, err := zk.conn.Children(zk.rootDir)
	return children, err
}

func (zk *ZkStorage) Update(data *protocol.Update) error {
	bytes, err := protocol.ToBytes(data.Post)
	if err != nil {
		return nil
	}
	_, err = zk.conn.Set(zk.getPath(data.TaskId), bytes, -1)
	return err
}

func (zk *ZkStorage) Create(data *protocol.Post) (string, error) {
	id := zk.getTaskId()
	bytes, err := protocol.ToBytes(data)
	if err != nil {
		return "", err
	}
	_, err = zk.conn.Create(zk.getPath(id), bytes, zk.flags, zk.acl)
	return id, err
}

func (zk *ZkStorage) Delete(data *protocol.Delete) error {
	err := zk.conn.Delete(zk.getPath(data.TaskId), -1)
	return err
}

func (zk *ZkStorage) DeleteAll() error {
	children, _, err := zk.conn.Children(zk.rootDir)
	if err != nil {
		return nil
	}

	for _, c := range children {
		err = zk.conn.Delete(c, -1)
		if err != nil {
			return err
		}
	}

	return zk.deleteDir(zk.conn, zk.rootDir)
}

func (zk *ZkStorage) createDir(conn *zkCli.Conn, dir string) error {
	dir = dir[1:]                    // remove leading "/"
	if strings.HasSuffix(dir, "/") { // remove tailing "/"
		dir = dir[:len(dir)-1]
	}
	paths := strings.Split(dir, "/")
	data := make([]byte, 0)

	// ignore all intermediate error
	conn.Create("/"+paths[0], data, zk.flags, zk.acl)
	cur := "/" + paths[0]
	for _, path := range paths[1 : len(paths)-1] {
		cur += "/" + path
		conn.Create(cur, data, zk.flags, zk.acl)
	}

	cur += "/" + paths[len(paths)-1]
	_, err := conn.Create(cur, data, zk.flags, zk.acl)
	return err
}

func (zk *ZkStorage) deleteDir(conn *zkCli.Conn, dir string) error {
	dir = dir[1:]                    // remove leading "/"
	if strings.HasSuffix(dir, "/") { // remove tailing "/"
		dir = dir[:len(dir)-1]
	}
	paths := strings.Split(dir, "/")
	for i := len(paths); i >= 1; i-- {
		cur := "/" + strings.Join(paths[:i], "/")
		err := conn.Delete(cur, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zk *ZkStorage) getPath(path string) string {
	return zk.rootDir + "/" + path
}

func (zk *ZkStorage) getTaskId() string {
	return strconv.FormatInt(time.Now().Unix(), 10) + "-" + zk.randString(6)
}

func (zk *ZkStorage) randString(length int) string {
	const chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := make([]byte, length)
	for i := 0; i < length; i++ {
		s[i] = chrs[rand.Intn(len(chrs))]
	}
	return string(s)
}
