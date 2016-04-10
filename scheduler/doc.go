package scheduler

// This is the doc guiding the implementation of the scheduler :
// http://mesos.apache.org/documentation/latest/high-availability-framework-guide/
// for now the design is : only the master will launch a task.

// TODO port number is not shown in UI
// TODO couple of choices to perform the replicated log (this is important to support large amount of data
// and reduce the burden of zookeeper) : 1. mesos itself has a replicated log for framework, but I didn't find
// a golang version; 2. use some mature projects like Apache Kalfka 3.  build my own (with some help of paxos/raft library)
//https://ramcloud.stanford.edu/~ongaro/userstudy/paxos.pdf   https://ramcloud.stanford.edu/~ongaro/userstudy/raft.pdf
// TODO consider to use the task state and host information from mesos-master. However probably it still needs its own
// state store because not every state here can be mapped to a state in mesos-master (ex. waiting for a re-start after a failure)
// TODO expose service discovery APIs, simulating mesos-dns. It can be in another module/microservices.
// TODO interesting "exec" feature - User can send command to container and it will return stduot the the user.
// considering to use either docker API or linux system call (ex. netns) to implement it. Currently this may require
// writing a customized executor first. Unfortunately mesos doesn't provide the ability of appending meta data to docker container,
// i.e., in mesos master there is no relationship between a docker container name/id and mesos task id. (see this :
// https://github.com/mesosphere/marathon/issues/717 )
// One possible solution is running a docker audit service on each mesos slave, which is responsible for mapping a task to
// a docker container (we can use port to locate web application); or we can set environment variable as task id for each docker
// container , and search the virable for service discovery.
// TODO support task scale-up / scale-down (faiure-over has already been supported)
// TODO support repeating tasks
// TODO support dependency graph
// TODO support multi-history version. Right now for each task only 1 history version is kept because
//		history data is stored with the same structure (runtime_store)
// TODO combine all store structures to a single object. re-design the interfaces.
// TODO use protobuf to redefine the schema and replace json encoding/decoding
// TODO add leader election
// TODO scalability (task group, scheduler group, etc.), this is important because it's a missing feature for
// current open source frameworks (ex. Marathon could not scale to millions of apps, because it saves meta data
// to many tasks within the same group to a single znode, which at some point will exceed the max size of znode.
// Instead of this storage model, it should also provide interfaces to save meta datas to other storage system than zookeeper)
// see :  https://mesosphere.github.io/marathon/docs/application-groups.html
// TODO http component + UI
// TODO separate APIs from core scheduler logics
// TODO more tests
// TODO tutorial, document, etc.
