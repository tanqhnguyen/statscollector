package statscollector

import (
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/stretchr/testify/mock"
	"github.com/tanqhnguyen/goinfluxdb"
)

// StatsCollector is an interface to interact with time series database
type StatsCollector interface {
	StorePoint(namespace string, tags map[string]string, fields map[string]interface{}, t time.Time)
	Close()
}

// NewInfluxDBCollectorFromEnvConfig returns a new collector that uses influxdb
func NewInfluxDBCollectorFromEnvConfig() *InfluxDBCollector {
	client, config := goinfluxdb.NewDefaultInfluxClient()
	return &InfluxDBCollector{
		client:   client,
		writeAPI: client.WriteApi("", config.Database),
	}
}

// InfluxDBCollector implements the StatsCollector interface using influxdb
type InfluxDBCollector struct {
	client   influxdb2.Client
	writeAPI api.WriteApi
}

// StorePoint stores a "point" (with tags and values) at a specific time
func (c *InfluxDBCollector) StorePoint(namespace string, tags map[string]string, fields map[string]interface{}, t time.Time) {
	p := influxdb2.NewPoint(namespace,
		tags,
		fields,
		t)
	c.writeAPI.WritePoint(p)
}

// Close frees all resources
func (c *InfluxDBCollector) Close() {
	c.client.Close()
	c.writeAPI.Close()
}

// NewMockCollector returns a new mock collector for using in tests
func NewMockCollector() *MockCollector {
	collector := &MockCollector{}

	collector.On("StorePoint", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

	return collector
}

// MockCollector is the "mock" version of StatsCollector. It can be used in tests to avoid calling
// the real db
type MockCollector struct {
	mock.Mock
}

// StorePoint does nothing
func (c *MockCollector) StorePoint(namespace string, tags map[string]string, fields map[string]interface{}, t time.Time) {
	c.Called(namespace, tags, fields, t)
}

// Close does nothing
func (c *MockCollector) Close() {
	c.Called()
}
