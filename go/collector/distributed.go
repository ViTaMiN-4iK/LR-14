package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/etcd/client/v3"
)

type ShardConfig struct {
	InstanceID string
	ShardID    int
	TotalShards int
	Sensors     []SensorType
	ETCDEndpoints []string
}

type DistributedCollector struct {
	config     ShardConfig
	etcdClient *clientv3.Client
	dataCh     chan interface{}
	stopCh     chan struct{}
	wg         sync.WaitGroup
	isLeader   bool
}

func NewDistributedCollector(config ShardConfig) (*DistributedCollector, error) {
	collector := &DistributedCollector{
		config:   config,
		dataCh:   make(chan interface{}, 1000),
		stopCh:   make(chan struct{}),
		isLeader: false,
	}

	if len(config.ETCDEndpoints) > 0 {
		client, err := clientv3.NewFromURLs(config.ETCDEndpoints)
		if err != nil {
			log.Printf("Warning: Failed to connect to etcd: %v", err)
		} else {
			collector.etcdClient = client
			if err := collector.register(); err != nil {
				log.Printf("Warning: Failed to register: %v", err)
			}
		}
	}

	return collector, nil
}

func (c *DistributedCollector) register() error {
	if c.etcdClient == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	ctx := context.Background()

	key := fmt.Sprintf("/medical/collectors/%s", c.config.InstanceID)
	value := fmt.Sprintf(`{"shard_id": %d, "total_shards": %d, "sensors": %v}`,
		c.config.ShardID, c.config.TotalShards, c.config.Sensors)

	_, err := c.etcdClient.Put(ctx, key, value)
	if err != nil {
		return err
	}

	lease, err := c.etcdClient.Grant(ctx, 30)
	if err != nil {
		return err
	}

	_, err = c.etcdClient.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}

	_, err = c.etcdClient.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}

	log.Printf("Registered collector %s with shard %d", c.config.InstanceID, c.config.ShardID)

	c.checkLeadership()

	return nil
}

func (c *DistributedCollector) checkLeadership() {
	if c.etcdClient == nil {
		return
	}

	ctx := context.Background()
	resp, err := c.etcdClient.Get(ctx, "/medical/leader", clientv3.WithFirstCreate()...)
	if err != nil {
		return
	}

	if len(resp.Kvs) == 0 {
		c.etcdClient.Put(ctx, "/medical/leader", c.config.InstanceID)
		c.isLeader = true
		log.Println("This instance is now the leader")
	}
}

func (c *DistributedCollector) startDataCollection() {
	emulator := NewEmulator(c.config.Sensors, c.dataCh)
	emulator.Start()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		emulator.Stop()
	}()
}

func (c *DistributedCollector) processData() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		count := 0
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case data, ok := <-c.dataCh:
				if !ok {
					return
				}
				count++
				jsonData, _ := json.Marshal(data)
				log.Printf("[Shard %d] %s", c.config.ShardID, string(jsonData))
			case <-ticker.C:
				if count > 0 {
					log.Printf("[Shard %d] Processed %d readings", c.config.ShardID, count)
					count = 0
				}
			case <-c.stopCh:
				return
			}
		}
	}()
}

func (c *DistributedCollector) startHealthCheck() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				if c.etcdClient != nil {
					ctx := context.Background()
					key := fmt.Sprintf("/medical/collectors/%s", c.config.InstanceID)
					c.etcdClient.Put(ctx, key, "active")
					log.Printf("[Shard %d] Health check OK", c.config.ShardID)
				}
			}
		}
	}()
}

func (c *DistributedCollector) Start() {
	log.Printf("Starting collector %s (shard %d/%d)",
		c.config.InstanceID, c.config.ShardID, c.config.TotalShards)

	c.startDataCollection()
	c.processData()
	c.startHealthCheck()
}

func (c *DistributedCollector) Stop() {
	close(c.stopCh)
	c.wg.Wait()

	if c.etcdClient != nil {
		ctx := context.Background()
		key := fmt.Sprintf("/medical/collectors/%s", c.config.InstanceID)
		c.etcdClient.Delete(ctx, key)
		c.etcdClient.Close()
	}

	log.Printf("Collector %s stopped", c.config.InstanceID)
}

func (c *DistributedCollector) GetShardAssignment() (int, []SensorType) {
	sensors := []SensorType{Pulse, Temperature, SpO2, BloodPressure}
	start := c.config.ShardID * len(sensors) / c.config.TotalShards
	end := (c.config.ShardID + 1) * len(sensors) / c.config.TotalShards
	return c.config.ShardID, sensors[start:end]
}

func RunDistributedExample() {
	fmt.Println("=== Distributed Collector Example ===")

	etcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpoints == "" {
		etcdEndpoints = "http://localhost:2379"
	}

	instanceID := fmt.Sprintf("collector-%d", os.Getpid()%1000)
	totalShards := 3

	collector, err := NewDistributedCollector(ShardConfig{
		InstanceID:     instanceID,
		ShardID:       0,
		TotalShards:   totalShards,
		Sensors:       []SensorType{Pulse, Temperature, SpO2, BloodPressure},
		ETCDEndpoints: []string{etcdEndpoints},
	})
	if err != nil {
		log.Printf("Failed to create collector: %v", err)
		return
	}

	collector.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Collector %s running. Press Ctrl+C to stop.\n", instanceID)
	<-sigCh

	fmt.Println("\nShutting down...")
	collector.Stop()
}
