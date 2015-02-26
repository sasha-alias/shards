package shards

import (
    "database/sql"
    _ "github.com/lib/pq"
    "hash/crc32"
    "math"
)

type ICluster interface {
    NextShard() (*sql.DB, string)
    HashStringShard(value string) (*sql.DB, string)
    ConnectStringShard(connstr string) (*sql.DB, string)
}

// Cluster constructor
func NewCluster(connstrs []string) (ICluster, error) {
    cluster := new(Cluster)
    cluster.databases = make(map[string]*sql.DB)
    cluster.index_mapping = make(map[int]string)
    for i, connstr := range connstrs {
        db, err := sql.Open("postgres", connstr)
        if err != nil{
            return nil, err
        }
        cluster.databases[connstr] = db
        cluster.index_mapping[i] = connstr
    }
    return cluster, nil
}

// Cluster of databases
type Cluster struct {
    databases map[string]*sql.DB
    index int
    index_mapping map[int]string
}

// Get next database from the cluster in case of round robin
func (self *Cluster) NextShard() (*sql.DB, string) {
    if self.index == len(self.databases)-1 {
        self.index = 0
    } else {
        self.index++
    }
    key := self.index_mapping[self.index]
    return self.databases[key], key
}

// Get shard corresponding to a hash of the value
// CRC32 hash algorithm is used for hash calculation
// Postgresql analogue is hash64(value, 'crc32') from pghashlib extension https://github.com/markokr/pghashlib
func (self *Cluster) HashStringShard(value string) (*sql.DB, string) {
    h := crc32.NewIEEE()
    h.Write([]byte(value))
    hash := float64(h.Sum32())
    shard_no := math.Mod(hash, float64(len(self.databases)))
    key := self.index_mapping[int(shard_no)]
    return self.databases[key], key
}

// Get shard instance by connection string
func (self *Cluster) ConnectStringShard(connstr string) (*sql.DB, string) {
    if shard, exists := self.databases[connstr]; exists {
        return shard, connstr
    } else {
        return nil, connstr
    }
}
