// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bookmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Bookmark represents a single bookmark entry for a topic-partition combination.
type Bookmark struct {
	Topic     string                 `json:"topic"`
	Partition string                 `json:"partition"`
	Offset    int                    `json:"offset"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// NewBookmark creates a new Bookmark with validation and default values
func NewBookmark(topic, partition string, offset int) (*Bookmark, error) {
	b := &Bookmark{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	if err := b.validate(); err != nil {
		return nil, err
	}

	return b, nil
}

// NewBookmarkWithTimestamp creates a new Bookmark with custom timestamp
func NewBookmarkWithTimestamp(topic, partition string, offset int, timestamp time.Time, metadata map[string]interface{}) (*Bookmark, error) {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	b := &Bookmark{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: timestamp,
		Metadata:  metadata,
	}

	if err := b.validate(); err != nil {
		return nil, err
	}

	return b, nil
}

// validate performs validation similar to __post_init__ in Python
func (b *Bookmark) validate() error {
	if strings.TrimSpace(b.Topic) == "" {
		return errors.New("topic must be a non-empty string")
	}
	if strings.TrimSpace(b.Partition) == "" {
		return errors.New("partition must be a non-empty string")
	}
	if b.Offset < 0 {
		return errors.New("offset must be a non-negative integer")
	}
	return nil
}

// TimestampUTC returns timestamp in UTC timezone
// Note: This is a simplified version. You'll need to implement TimestampUtils.ToUTC
// or use a timezone conversion library for the exact Eastern timezone conversion
func (b *Bookmark) TimestampUTC() time.Time {
	// TODO: Implement proper timezone conversion from US/Eastern to UTC
	// For now, assuming the timestamp is already in the correct timezone
	return b.Timestamp.UTC()
}

// TimestampUTCISO returns timestamp as ISO string in UTC
func (b *Bookmark) TimestampUTCISO() string {
	return b.TimestampUTC().Format(time.RFC3339)
}

// ToDict converts bookmark to a map (dictionary equivalent)
func (b *Bookmark) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"topic":     b.Topic,
		"partition": b.Partition,
		"offset":    b.Offset,
		"timestamp": b.Timestamp.Format(time.RFC3339),
		"metadata":  b.Metadata,
	}
}

// ToJSON converts bookmark to JSON string
func (b *Bookmark) ToJSON() (string, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// FromDict creates a bookmark from a map (dictionary equivalent)
func FromDict(data map[string]interface{}) (*Bookmark, error) {
	topic, ok := data["topic"].(string)
	if !ok {
		return nil, errors.New("invalid or missing topic")
	}

	partition, ok := data["partition"].(string)
	if !ok {
		return nil, errors.New("invalid or missing partition")
	}

	offset, ok := data["offset"].(float64) // JSON numbers are float64
	if !ok {
		return nil, errors.New("invalid or missing offset")
	}

	var timestamp time.Time
	if tsStr, exists := data["timestamp"].(string); exists {
		var err error
		timestamp, err = time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp format: %v", err)
		}
	} else {
		timestamp = time.Now()
	}

	metadata, ok := data["metadata"].(map[string]interface{})
	if !ok {
		metadata = make(map[string]interface{})
	}

	return NewBookmarkWithTimestamp(topic, partition, int(offset), timestamp, metadata)
}

// FromJSON creates a bookmark from JSON string
func FromJSON(jsonStr string) (*Bookmark, error) {
	var b Bookmark
	if err := json.Unmarshal([]byte(jsonStr), &b); err != nil {
		return nil, err
	}

	if err := b.validate(); err != nil {
		return nil, err
	}

	return &b, nil
}
