package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

//定义常量
const (
	BucketSize = 3   //每个bucket的最大容量
	IdSize     = 20  //ID的大小
	NumPeers   = 100 //每个节点中的Peer数量
	NumKeys    = 200 //随机生成的字符串数量
)

//添加DHT结构体
type DHT struct {
	kb *KBucket
}

//添加Peer结构体
type Peer struct {
	node  Node
	kb    *KBucket
	store map[[IdSize]byte][]byte //用map保存键值对
	dht   DHT
}

type Node struct {
	id   [IdSize]byte //节点ID长度为IdSize
	data interface{}  //节点存储的数据
}

type Bucket struct {
	nodes []Node //节点列表
}

type KBucket struct {
	buckets  [IdSize * 8]*Bucket //K-Bucket中存放bucket 的数组
	selfId   [IdSize]byte        // 自身节点的ID
	maxNodes int                 // 每个bucket的最大节点数量
}

func NewBucket() *Bucket {
	return &Bucket{
		nodes: make([]Node, 0, BucketSize), // 初始化节点列表，容量为BUCKET_SIZE
	}
}

func (b *Bucket) Len() int {
	return len(b.nodes) // 返回节点列表的长度
}

func (b *Bucket) insertNode(n Node) bool {
	if len(b.nodes) >= BucketSize { // 超过容量，无法添加节点
		return false
	}
	for i, x := range b.nodes { // 节点已存在，则更新数据
		if x.id == n.id {
			b.nodes[i].data = n.data
			return true
		}
	}
	b.nodes = append(b.nodes, n) // 添加新节点
	return true
}

func (b *Bucket) UpdateNode(n Node) {
	for i, x := range b.nodes { // 更新节点数据
		if x.id == n.id {
			b.nodes[i].data = n.data
		}
	}
}

func (b *Bucket) RemoveNode(id [IdSize]byte) bool {
	for i, x := range b.nodes { // 删除节点
		if x.id == id {
			b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
			return true
		}
	}
	return false // 节点不存在，无法删除
}

func (b *Bucket) FindNode(id [IdSize]byte) (Node, bool) {
	for _, x := range b.nodes {
		if x.id == id { // 查找节点
			return x, true
		}
	}
	return Node{}, false // 节点不存在
}

//用于创建随机字符串函数
func NewKBucket(nodeId [IdSize]byte, maxNodes int) *KBucket {
	kb := &KBucket{
		selfId:   nodeId,
		maxNodes: maxNodes,
	}
	for i := range kb.buckets { // 初始化 bucket
		kb.buckets[i] = NewBucket()
	}
	return kb
}

func (kb *KBucket) nLeadingZeros(id [IdSize]byte) int {
	zeros := 0
	for i := 0; i < IdSize; i++ {
		for j := 7; j >= 0; j-- {
			if (id[i]>>uint(j))&0x01 != 0 {
				return zeros // 返回 ID 中前导零的个数
			}
			zeros++
		}
	}
	return zeros
}

func (kb *KBucket) GetBucket(pos int) *Bucket { // 获取指定位置的bucket
	return kb.buckets[pos]
}

func (kb *KBucket) calcBucketIndex(id [IdSize]byte) int {
	zeros := kb.nLeadingZeros(id)
	return IdSize*8 - 1 - zeros // 计算 bucket 的索引值
}

func (kb *KBucket) insertNode(n Node) bool {
	if n.id == kb.selfId { // 自身节点不需要添加
		return true
	}
	pos := kb.calcBucketIndex(n.id) // 计算节点应该放置的 bucket 的索引值
	bucket := kb.GetBucket(pos)     // 获取对应的 bucket
	if bucket.insertNode(n) {       // 直接添加节点到 bucket 中
		return true
	}
	if pos == IdSize*8-1 { // 节点与自身节点相同，无法添加
		return false
	}
	// Split the bucket.
	newBucket := NewBucket()
	kb.buckets[pos+1] = newBucket
	for _, node := range bucket.nodes[kb.maxNodes/2:] { // 将超过容量的节点移动到新的 bucket 中
		if pos+1 == kb.calcBucketIndex(node.id) {
			newBucket.insertNode(node)
		}
	}
	bucket.nodes = bucket.nodes[:kb.maxNodes/2] // 删除超过容量的节点
	if pos == kb.calcBucketIndex(kb.selfId) {   // 尝试重新添加节点
		return kb.insertNode(n)
	}
	return newBucket.insertNode(n) // 将节点添加到新的 bucket 中
}

func (kb *KBucket) RemoveNode(id [IdSize]byte) bool {
	pos := kb.calcBucketIndex(id)
	bucket := kb.GetBucket(pos)
	if bucket.RemoveNode(id) { // 从 bucket 中删除节点
		return true
	}
	return false
}

func (kb *KBucket) printBucketContents(bucket *Bucket) { // 打印bucket 中节点的 ID
	for i, node := range bucket.nodes {
		fmt.Printf("序号: %d nodeID: %x\n", i, node.id)
	}
}

func (kb *KBucket) PrintKBucket() { // 打印整个 K-Bucket 中的所有节点
	for i := range kb.buckets {
		bucket := kb.GetBucket(i)
		if bucket.Len() > 0 {
			fmt.Printf("Bucket %d:\n", i)
			kb.printBucketContents(bucket)
		}
	}
}

func NewPeer(id [IdSize]byte) *Peer {
	kb := NewKBucket(id, BucketSize)
	return &Peer{
		node:  Node{id: id},
		kb:    kb,
		store: make(map[[IdSize]byte][]byte),
		dht:   DHT{kb: kb},
	}
}

func (p *Peer) SetValue(key, value []byte) bool {
	if key == nil || value == nil {
		panic("key or value is empty")
	}
	hash := sha1.Sum(value)
	if binary.BigEndian.Uint64(key) != binary.BigEndian.Uint64(hash[:]) {
		return false
	}
	if _, ok := p.store[hash]; ok {
		return true
	}
	p.store[hash] = value
	pos := p.kb.calcBucketIndex(hash)
	bucket := p.kb.GetBucket(pos)
	nodes := bucket.nodes
	if len(nodes) > 2 {
		nodes = nodes[:2]
	}
	for _, node := range nodes {
		peer := node.data.(*Peer)
		peer.SetValue(key, value)
	}
	return true
}

func (p *Peer) GetValue(key [IdSize]byte) []byte {
	if value, ok := p.store[key]; ok {
		return value
	}
	pos := p.kb.calcBucketIndex(key)
	bucket := p.kb.GetBucket(pos)
	nodes := bucket.nodes
	if len(nodes) > 2 {
		nodes = nodes[:2]
	}
	for _, node := range nodes {
		peer := node.data.(*Peer)
		value := peer.GetValue(key)
		if value != nil {
			return value
		}
	}
	return nil
}

//用来创建随机字符串
func randomString() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := rand.Intn(30) + 1
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	//初始化100个节点
	peers := make([]*Peer, NumPeers)
	for i := 0; i < NumPeers; i++ {
		id := [IdSize]byte{}
		rand.Read(id[:])
		peers[i] = NewPeer(id)
	}
	// 随机生成200个字符串并计算哈希值
	keys := make([][IdSize]byte, NumKeys)
	for i := 0; i < NumKeys; i++ {
		value := randomString()
		hash := sha1.Sum([]byte(value))
		keys[i] = hash
		peerIdx := rand.Intn(NumPeers)
		peers[peerIdx].SetValue(hash[:], []byte(value))
	}
	// 随机选择100个key进行GetValue操作
	for i := 0; i < 100; i++ {
		keyIdx := rand.Intn(NumKeys)
		peerIdx := rand.Intn(NumPeers)
		value := peers[peerIdx].GetValue(keys[keyIdx])
		if value != nil {
			fmt.Printf("true:节点%2d找到了 Key: %x 对应的值: %s\n", peerIdx, keys[keyIdx], string(value))
		} else {
			fmt.Printf("false:节点%2d找不到 Key: %x 对应的值\n", peerIdx, keys[keyIdx])
		}
	}
}
