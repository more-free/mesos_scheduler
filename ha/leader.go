package ha

import (
	"encoding/json"
	zkCli "github.com/samuel/go-zookeeper/zk"
	"log"
	"sort"
	"time"
)

type Host struct {
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

func (h *Host) toBytes() ([]byte, error) {
	return json.Marshal(h)
}

func fromBytes(bytes []byte) (*Host, error) {
	var host Host
	err := json.Unmarshal(bytes, &host)
	if err != nil {
		return nil, err
	} else {
		return &host, nil
	}
}

type LeaderStatusUpdater interface {
	LeaderElected(newLeader *Host)
	LeaderLost(oldLeader *Host)
}

type LeaderElection interface {
	ElectLeader() error // blocking or non-blocking
	Close()
}

type ZkLeaderElection struct {
	servers     []string // zk servers
	root        string   // "/__leader"
	acl         []zkCli.ACL
	conn        *zkCli.Conn
	connTimeout time.Duration
	connChan    <-chan zkCli.Event // chan gets closed if session is lost
	host        *Host              // self
	updater     LeaderStatusUpdater
	closeChan   chan bool
}

func NewZkLeaderElection(servers []string, host *Host, updater LeaderStatusUpdater, connTimeout time.Duration) (LeaderElection, error) {
	conn, connChan, err := zkCli.Connect(servers, connTimeout)
	if err != nil {
		return nil, err
	}

	root := "/__leader"
	acls := zkCli.WorldACL(zkCli.PermAll)
	exists, _, err := conn.Exists(root)
	if !exists {
		_, err = conn.Create(root, make([]byte, 0), int32(0), acls)
		if err != nil {
			return nil, err
		}
	}

	return &ZkLeaderElection{
		servers:     servers,
		root:        root,
		acl:         acls,
		conn:        conn,
		connTimeout: connTimeout,
		host:        host,
		updater:     updater,
		connChan:    connChan,
		closeChan:   make(chan bool),
	}, nil
}

func (zk *ZkLeaderElection) Close() {
	close(zk.closeChan)
	zk.conn.Close()
}

// need to avoid the nerd effect only if it is used by a large number of nodes
// http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
func (zk *ZkLeaderElection) ElectLeader() error {
	_, err := zk.register()
	if err != nil {
		return err
	}
	log.Println("registered with zk :", zk.host)

	leader, leaderChan, err := zk.getAndWatchLeader()
	if err != nil {
		return err
	}
	log.Println("got leader from zookeeper :", leader)

	// must run it within a go routine due to the recursive implementation
	go zk.monitor(leader, leaderChan)
	log.Println("start monitoring leader status change")

	return nil
}

func (zk *ZkLeaderElection) register() (string, error) {
	data, err := zk.host.toBytes()
	if err != nil {
		return "", err
	}

	path, err := zk.conn.Create(zk.root+"/node#",
		data,
		zkCli.FlagEphemeral|zkCli.FlagSequence,
		zk.acl,
	)
	if err != nil {
		return "", err
	}
	return path, nil
}

func (zk *ZkLeaderElection) getAndWatchLeader() (*Host, <-chan zkCli.Event, error) {
	children, _, err := zk.conn.Children(zk.root)
	if err != nil {
		return nil, make(chan zkCli.Event), err
	}
	leaderPath := zk.getMinChild(children)
	leaderData, _, leaderChan, err := zk.conn.GetW(zk.root + "/" + leaderPath)
	if err != nil {
		return nil, make(chan zkCli.Event), err
	}
	leader, err := fromBytes(leaderData)
	if err != nil {
		return nil, make(chan zkCli.Event), err
	}
	zk.updater.LeaderElected(leader)
	return leader, leaderChan, nil
}

// monitor leader change and connection loss
func (zk *ZkLeaderElection) monitor(leader *Host, leaderChan <-chan zkCli.Event) {
	for {
		select {
		case event := <-leaderChan:
			if event.Type == zkCli.EventNodeDeleted {
				zk.updater.LeaderLost(leader)
				err := zk.ElectLeader() // re-run ElectLeader, which must be non-blocking
				if err != nil {
					log.Fatalln(err)
				}
				return
			}

		case event := <-zk.connChan:
			// optional step : rebuild connection when connection lost
			// TODO a retry or max_timeout parameter can be used to control this behavior
			if event.Type == zkCli.EventSession && event.State == zkCli.StateDisconnected {
				conn, connChan, err := zkCli.Connect(zk.servers, zk.connTimeout)
				if err != nil {
					log.Fatalln("Cannot connect to zookeeper : ", zk.servers, err)
				} else {
					zk.conn = conn
					zk.connChan = connChan
					err := zk.ElectLeader() // re-run ElectLeader, which must be non-blocking
					if err != nil {
						log.Fatalln(err)
					}
				}
				return
			}

		case <-zk.closeChan:
			// leader election was stopped by calling Close()
			log.Println("quit leader election : ", zk.host)
			return
		}
	}
}

// TODO it should handle a corner case : sequential number may overflow
func (zk *ZkLeaderElection) getMinChild(children []string) string {
	sort.Strings(children)
	return children[0]
}
