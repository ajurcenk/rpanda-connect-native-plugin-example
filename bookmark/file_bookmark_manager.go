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
	"github.com/redpanda-data/benthos/v4/public/service"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// BookmarkManager manages bookmarks with file-based persistence
type BookmarkManager struct {
	filePath  string
	bookmarks map[string]*Bookmark // key: "topic:partition"
	mutex     sync.RWMutex
}

// BookmarkFile represents the structure saved to/loaded from file
type BookmarkFile struct {
	Version   string      `json:"version"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	Bookmarks []*Bookmark `json:"bookmarks"`
}

// NewBookmarkManager creates a new bookmark manager
func NewBookmarkManager(filePath string) *BookmarkManager {
	return &BookmarkManager{
		filePath:  filePath,
		bookmarks: make(map[string]*Bookmark),
	}
}

// generateKey creates a unique key for topic-partition combination
func (bm *BookmarkManager) generateKey(topic, partition string) string {
	return fmt.Sprintf("%s:%s", topic, partition)
}

// AddBookmark adds or updates a bookmark
func (bm *BookmarkManager) AddBookmark(bookmark *Bookmark) error {
	if bookmark == nil {
		return errors.New("bookmark cannot be nil")
	}

	if err := bookmark.validate(); err != nil {
		return fmt.Errorf("invalid bookmark: %w", err)
	}

	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	key := bm.generateKey(bookmark.Topic, bookmark.Partition)
	bm.bookmarks[key] = bookmark

	return nil
}

// GetBookmark retrieves a bookmark by topic and partition
func (bm *BookmarkManager) GetBookmark(topic, partition string) (*Bookmark, error) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	key := bm.generateKey(topic, partition)
	bookmark, exists := bm.bookmarks[key]
	if !exists {
		return nil, fmt.Errorf("bookmark not found for topic: %s, partition: %s", topic, partition)
	}

	return bookmark, nil
}

// GetAllBookmarks returns all bookmarks
func (bm *BookmarkManager) GetAllBookmarks() []*Bookmark {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	bookmarks := make([]*Bookmark, 0, len(bm.bookmarks))
	for _, bookmark := range bm.bookmarks {
		bookmarks = append(bookmarks, bookmark)
	}

	// Sort by topic, then by partition for consistent ordering
	sort.Slice(bookmarks, func(i, j int) bool {
		if bookmarks[i].Topic == bookmarks[j].Topic {
			return bookmarks[i].Partition < bookmarks[j].Partition
		}
		return bookmarks[i].Topic < bookmarks[j].Topic
	})

	return bookmarks
}

// GetBookmarksByTopic returns all bookmarks for a specific topic
func (bm *BookmarkManager) GetBookmarksByTopic(topic string) []*Bookmark {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	var bookmarks []*Bookmark
	for _, bookmark := range bm.bookmarks {
		if bookmark.Topic == topic {
			bookmarks = append(bookmarks, bookmark)
		}
	}

	// Sort by partition
	sort.Slice(bookmarks, func(i, j int) bool {
		return bookmarks[i].Partition < bookmarks[j].Partition
	})

	return bookmarks
}

// RemoveBookmark removes a bookmark by topic and partition
func (bm *BookmarkManager) RemoveBookmark(topic, partition string) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	key := bm.generateKey(topic, partition)
	if _, exists := bm.bookmarks[key]; !exists {
		return fmt.Errorf("bookmark not found for topic: %s, partition: %s", topic, partition)
	}

	delete(bm.bookmarks, key)
	return nil
}

// UpdateOffset updates the offset for an existing bookmark
func (bm *BookmarkManager) UpdateOffset(topic, partition string, offset int) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	key := bm.generateKey(topic, partition)
	bookmark, exists := bm.bookmarks[key]
	if !exists {
		return fmt.Errorf("bookmark not found for topic: %s, partition: %s", topic, partition)
	}

	if offset < 0 {
		return errors.New("offset must be non-negative")
	}

	bookmark.Offset = offset
	bookmark.Timestamp = time.Now()

	return nil
}

// Count returns the number of bookmarks
func (bm *BookmarkManager) Count() int {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	return len(bm.bookmarks)
}

// Clear removes all bookmarks
func (bm *BookmarkManager) Clear() {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	bm.bookmarks = make(map[string]*Bookmark)
}

// SaveToFile saves all bookmarks to the specified file
func (bm *BookmarkManager) SaveToFile() error {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	// Create directory if it doesn't exist
	dir := filepath.Dir(bm.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Prepare bookmark file structure
	bookmarkFile := BookmarkFile{
		Version:   "1.0",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Bookmarks: make([]*Bookmark, 0, len(bm.bookmarks)),
	}

	// Convert map to slice for JSON serialization
	for _, bookmark := range bm.bookmarks {
		bookmarkFile.Bookmarks = append(bookmarkFile.Bookmarks, bookmark)
	}

	// Sort bookmarks for consistent file output
	sort.Slice(bookmarkFile.Bookmarks, func(i, j int) bool {
		if bookmarkFile.Bookmarks[i].Topic == bookmarkFile.Bookmarks[j].Topic {
			return bookmarkFile.Bookmarks[i].Partition < bookmarkFile.Bookmarks[j].Partition
		}
		return bookmarkFile.Bookmarks[i].Topic < bookmarkFile.Bookmarks[j].Topic
	})

	// Marshal to JSON with indentation
	data, err := json.MarshalIndent(bookmarkFile, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal bookmarks: %w", err)
	}

	// Write to temporary file first, then rename (atomic operation)
	tempFile := bm.filePath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tempFile, bm.filePath); err != nil {
		os.Remove(tempFile) // Clean up temp file
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// LoadFromFile loads bookmarks from the specified file
func (bm *BookmarkManager) LoadFromFile() error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	// Check if file exists
	if _, err := os.Stat(bm.filePath); errors.Is(err, fs.ErrNotExist) {
		// File doesn't exist, start with empty bookmarks
		return nil
	}

	// Read file
	data, err := os.ReadFile(bm.filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Parse JSON
	var bookmarkFile BookmarkFile
	if err := json.Unmarshal(data, &bookmarkFile); err != nil {
		return fmt.Errorf("failed to unmarshal bookmarks: %w", err)
	}

	// Clear existing bookmarks and load from file
	bm.bookmarks = make(map[string]*Bookmark)

	// Validate and add each bookmark
	for _, bookmark := range bookmarkFile.Bookmarks {
		if err := bookmark.validate(); err != nil {
			return fmt.Errorf("invalid bookmark in file: %w", err)
		}

		key := bm.generateKey(bookmark.Topic, bookmark.Partition)
		bm.bookmarks[key] = bookmark
	}

	return nil
}

// FileExists checks if the bookmark file exists
func (bm *BookmarkManager) FileExists() bool {
	_, err := os.Stat(bm.filePath)
	return err == nil
}

// GetFilePath returns the file path being used
func (bm *BookmarkManager) GetFilePath() string {
	return bm.filePath
}

// String returns a string representation of the manager
func (bm *BookmarkManager) String() string {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	return fmt.Sprintf("BookmarkManager{filePath: %s, bookmarks: %d}", bm.filePath, len(bm.bookmarks))
}

func BookmarkFileManagerConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewObjectField("bookmarks_file",
			service.NewStringField("path").
				Description("The bookmark path.").
				Description("The file based bookmarks manager configuration")),
	}
}
