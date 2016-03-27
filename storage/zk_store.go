package storage

// leader handles all "mutable" requests, while followers handle read requests only, and forward "mutable" requests
// to leader
// TODO to make it scalable :
// 1. storage hierarchy should be splitted to keep each part small
// 2. don't always rely on leader to assign task to mesos

import (
	"encoding/json"
	"fmt"
	"github.com/more-free/mesos_scheduler/protocol"
	zkCli "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Storage interface {
	Get(*protocol.Get) (*protocol.Post, error)
	GetAll() ([]*protocol.Post, error)
	Update(*protocol.Update) error
	Post(*protocol.Post) (*protocol.Get, error) // create new and return unique task meta data for future query
	Delete(*protocol.Delete) error
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
	err := zk.deleteDir(zk.conn, zk.rootDir)
	if err != nil {
		return err
	}
	zk.conn.Close()
	return nil
}

func (zk *ZkStorage) Get(data *protocol.Get) (*protocol.Post, error) {
	return zk.getById(data.TaskId)
}

func (zk *ZkStorage) getById(id string) (*protocol.Post, error) {
	bytes, _, err := zk.conn.Get(zk.getPath(id))
	if err != nil {
		return nil, err
	}
	var post protocol.Post
	err = json.Unmarshal(bytes, &post)
	if err != nil {
		return nil, err
	}
	return &post, nil
}

func (zk *ZkStorage) GetAll() ([]*protocol.Post, error) {
	children, _, err := zk.conn.Children(zk.rootDir)
	if err != nil {
		return make([]*protocol.Post, 0), err
	}

	posts := make([]*protocol.Post, len(children))
	for i := 0; i < len(children); i++ {
		post, err := zk.getById(children[i])
		if err == nil {
			posts[i] = post
		}
	}
	return posts, err
}

func (zk *ZkStorage) Update(data *protocol.Update) error {
	bytes, err := protocol.ToBytes(data.Post)
	if err != nil {
		return nil
	}
	_, err = zk.conn.Set(zk.getPath(data.TaskId), bytes, -1)
	return err
}

func (zk *ZkStorage) Post(data *protocol.Post) (*protocol.Get, error) {
	id := zk.getTaskId()
	bytes, err := protocol.ToBytes(data)
	if err != nil {
		return nil, err
	}
	_, err = zk.conn.Create(zk.getPath(id), bytes, zk.flags, zk.acl)
	return &protocol.Get{id}, err
}

func (zk *ZkStorage) Delete(data *protocol.Delete) error {
	err := zk.conn.Delete(zk.getPath(data.TaskId), -1)
	return err
}

// delete all children znodes (but not the parent znodes)
func (zk *ZkStorage) DeleteAll() error {
	children, _, err := zk.conn.Children(zk.rootDir)
	if err != nil {
		return nil
	}

	for _, c := range children {
		err = zk.conn.Delete(zk.getPath(c), -1)
		if err != nil {
			return err
		}
	}
	return nil
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
