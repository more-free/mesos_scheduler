package ha

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os/exec"
	"testing"
	"time"
)

type statusUpdater struct {
	t                 *testing.T
	expectedLeader    *Host
	leaderElectedCall int
	leaderLostCall    int
}

func newStatusUpdater(t *testing.T) *statusUpdater {
	return &statusUpdater{t, nil, 0, 0}
}

func (u *statusUpdater) LeaderElected(newLeader *Host) {
	u.expectedLeader = newLeader
	u.leaderElectedCall++
}

func (u *statusUpdater) LeaderLost(oldLeader *Host) {
	u.expectedLeader = nil
	u.leaderLostCall++
}

func TestLeaderElection(t *testing.T) {
	out, err := exec.Command("bash", "-c", "echo ruok | nc localhost 2181").Output()
	if err != nil || string(out) != "imok" {
		log.Println("zookeeper is not running on localhost:2181. Pass the test")
		return
	}

	servers := []string{"localhost:2181"}
	leader := &Host{"leader", 3333}
	updater := newStatusUpdater(t)

	le, err := NewZkLeaderElection(
		servers, leader, updater, time.Second*3,
	)
	assert.Equal(t, nil, err)
	err = le.ElectLeader()
	defer le.Close()
	assert.Equal(t, nil, err)
	time.Sleep(time.Second * 1)
	assert.Equal(t, leader, updater.expectedLeader)

	follower := &Host{"follower", 3334}
	le2, err := NewZkLeaderElection(
		servers, follower, updater, time.Second*3,
	)
	assert.Equal(t, nil, err)
	err = le2.ElectLeader()
	defer le2.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, leader, updater.expectedLeader)
	assert.Equal(t, 2, updater.leaderElectedCall)
}

func TestLeaderElectionWithLeaderLost(t *testing.T) {
	out, err := exec.Command("bash", "-c", "echo ruok | nc localhost 2181").Output()
	if err != nil || string(out) != "imok" {
		log.Println("zookeeper is not running on localhost:2181. Pass the test")
		return
	}

	servers := []string{"localhost:2181"}
	leader := &Host{"leader", 3333}
	updater := newStatusUpdater(t)

	le, err := NewZkLeaderElection(
		servers, leader, updater, time.Second*3,
	)
	assert.Equal(t, nil, err)
	err = le.ElectLeader()
	assert.Equal(t, nil, err)
	time.Sleep(time.Second * 1)
	assert.Equal(t, leader, updater.expectedLeader)

	// add follower
	follower := &Host{"follower", 3334}
	le2, err := NewZkLeaderElection(
		servers, follower, updater, time.Second*3,
	)
	assert.Equal(t, nil, err)
	err = le2.ElectLeader()
	defer le2.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, updater.leaderElectedCall)

	// add another follower
	follower2 := &Host{"follower2", 3335}
	le3, err := NewZkLeaderElection(
		servers, follower2, updater, time.Second*3,
	)
	assert.Equal(t, nil, err)
	err = le3.ElectLeader()
	defer le3.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, updater.leaderElectedCall)

	// disconnect leader
	le.Close()
	time.Sleep(time.Second * 1)

	assert.Equal(t, follower, updater.expectedLeader)
	assert.Equal(t, 2, updater.leaderLostCall)
	assert.Equal(t, 5, updater.leaderElectedCall)
}

func TestLeaderElectionWithConnectionLost(t *testing.T) {

}
