/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/cihub/seelog"
	"strconv"
	"time"
)

type ZooKeeperOffsetClient struct {
	app             	*ApplicationContext
	cluster            	string
	conn    			*zk.Conn
	zkOffsetTicker 		*time.Ticker
}

func NewZooKeeperOffsetClient(app *ApplicationContext, cluster string) (*ZooKeeperOffsetClient, error) {
	zkhosts := make([]string, len(app.Config.Kafka[cluster].Zookeepers))
	for i, host := range app.Config.Kafka[cluster].Zookeepers {
		zkhosts[i] = fmt.Sprintf("%s:%v", host, app.Config.Kafka[cluster].ZookeeperPort)
	}
	zkconn, _, err := zk.Connect(zkhosts, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &ZooKeeperOffsetClient{
		app:     app,
		cluster: cluster,
		conn:    zkconn,
	}

	// Now get the first set of offsets and start a goroutine to continually check them
	client.getOffsets(client.app.Config.Kafka[cluster].ZookeeperOffsetPaths)
	client.zkOffsetTicker = time.NewTicker(time.Duration(client.app.Config.Tickers.ZooKeeperOffsets) * time.Second)
	go func() {
		for _ = range client.zkOffsetTicker.C {
			client.getOffsets(client.app.Config.Kafka[cluster].ZookeeperOffsetPaths)
		}
	}()

	return client, nil
}


func (zkOffsetClient *ZooKeeperOffsetClient) getOffsetsForConsumer(consumer string, consumerPath string) {
	children, _, err := zkOffsetClient.conn.Children(consumerPath + "/offsets")
	switch {
	case err == nil:
		for _, child := range children {
			zkOffsetClient.getOffsetsForTopic(consumer, child, consumerPath + "/offsets" + "/" + child)
		}
	case err ==  zk.ErrNoNode:
		// it is OK as the offsets may not be managed by ZK
		log.Infof("This consumer's offset is not managed by ZK: " + consumer)
		return
	default:
		panic(err)
	}
}

func (zkOffsetClient *ZooKeeperOffsetClient) getOffsetsForTopic(consumer string, topic string, topicPath string) {
	children, _, err := zkOffsetClient.conn.Children(topicPath)
	switch {
	case err == nil:
		for _, child := range children {
			partition, _ := strconv.Atoi(child)
			zkOffsetClient.getOffsetsForPartition(consumer, topic,  partition, topicPath + "/" + child)
		}
	default:
		panic(err)
	}
}

func (zkOffsetClient *ZooKeeperOffsetClient) getOffsetsForPartition(consumer string, topic string, partition int, partitionPath string) {

	zkNodeStat := &zk.Stat {}
	offsetStr, zkNodeStat, err := zkOffsetClient.conn.Get(partitionPath)
	switch {
	case err == nil:
		offset, _ := strconv.Atoi(string(offsetStr))
		fmt.Printf("About to sync ZK based offset: [%s,%s,%v]::[%v,%v]\n", consumer, topic, partition, offset, zkNodeStat.Mtime)
		partitionOffset := &PartitionOffset{
			Cluster:   zkOffsetClient.cluster,
			Topic:     topic,
			Partition: int32(partition),
			Group:     consumer,
			Timestamp: int64(zkNodeStat.Mtime),
			Offset:    int64(offset),
		}
		timeoutSendOffset(zkOffsetClient.app.Storage.offsetChannel, partitionOffset, 1)
	default:
		panic(err)
	}
}

func (zkOffsetClient *ZooKeeperOffsetClient) getOffsets(paths []string) {

	for _, path := range paths {
		exists, _, err := zkOffsetClient.conn.Exists(path)
		switch {
		case err == nil:
			if !exists {
				// we don't tolerate configuration error
				panic(err)
			}

			children, _, err := zkOffsetClient.conn.Children(path)
			switch {
			case err == nil:
				for _, child := range children {
					zkOffsetClient.getOffsetsForConsumer(child, path + "/"  + child)
				}
			default:
				panic(err)
			}

		default:
			panic(err)
		}
	}
}

func (zkOffsetClient *ZooKeeperOffsetClient) Stop() {
	zkOffsetClient.conn.Close()
}
