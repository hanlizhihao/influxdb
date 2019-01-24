// Code generated by go-bindata. DO NOT EDIT.
// sources:
// system.json (30.129kB)

package proto

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func bindataRead(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	clErr := gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}
	if clErr != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type asset struct {
	bytes  []byte
	info   os.FileInfo
	digest [sha256.Size]byte
}

type bindataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

func (fi bindataFileInfo) Name() string {
	return fi.name
}
func (fi bindataFileInfo) Size() int64 {
	return fi.size
}
func (fi bindataFileInfo) Mode() os.FileMode {
	return fi.mode
}
func (fi bindataFileInfo) ModTime() time.Time {
	return fi.modTime
}
func (fi bindataFileInfo) IsDir() bool {
	return false
}
func (fi bindataFileInfo) Sys() interface{} {
	return nil
}

var _systemJson = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xec\x5d\xfb\x6f\xdb\xba\x15\xfe\x3d\x7f\x05\xa1\x61\x40\x0a\xc4\xb5\x65\xcb\x4f\xa0\x77\xe8\x4d\x9b\xe1\x62\xbb\x5d\xb1\xb6\xd8\x0f\x4d\x90\xd0\xe2\x91\x4d\x44\x22\x35\x92\xf2\x63\x5d\xff\xf7\x81\x92\x65\xcb\xb2\xe8\xd9\x89\x9f\xd7\x0a\xd0\x46\xe2\xf3\x1c\xf2\x93\xbe\xef\x50\x94\xf2\xe3\x0a\x21\x8b\xe1\x00\xac\x1e\xb2\xe4\x54\x2a\x08\xac\x1b\x9d\x46\x89\x4e\xa9\x2d\xff\xd8\x49\x1e\xc1\x72\xd8\xe7\x58\x10\x69\xf5\xd0\xf7\x2b\x84\x10\xfa\x11\xff\x9f\xcd\xb3\x7a\xf3\xc4\x4c\x17\x5f\x16\x5d\xa4\x15\x40\xba\x82\x86\x8a\x72\xa6\x0b\xbc\x47\x2e\xf7\x7d\x70\xf5\x39\xe2\x1e\x8a\x24\x78\x91\x8f\x46\x54\x46\xd8\xa7\xff\xc1\x3a\x5d\x22\x8f\x0b\x14\x70\x46\x15\x17\x94\x0d\xd0\x94\x47\x02\xad\x36\xed\x82\xef\x2f\x4c\x4c\x7e\x7e\x64\x8e\x17\x6e\x36\x9c\x7e\xad\xe6\xb5\x49\xb3\xef\x36\x6b\xb5\x5a\xa6\x91\xb8\xd4\xc4\xea\xa1\x5a\x2e\x6d\x5a\x90\x36\xb6\x7a\xc8\xae\xe7\x12\x87\x3a\x31\x93\xf4\xf3\x66\x23\x73\xea\x9d\x6e\xa7\xdb\xd8\xca\x1c\xbb\xc0\x9c\xc6\x6e\xac\x71\x1c\xaf\xde\xf4\x8c\xd6\xe4\x7b\x31\x59\xb3\xa3\xb1\x71\x5a\xed\x7e\xc3\x3c\x55\xcd\xc3\x5a\xd3\x74\x9a\xc4\x33\xcf\x54\xfb\xb0\xd6\xb4\xea\x7d\xa7\x6d\x9e\xa9\x22\xdc\xe4\x7b\x36\xe1\xa6\xf1\x02\x6b\x9c\x4e\x93\x98\x67\xaa\x08\x37\xfb\xb4\x86\xb4\xfa\x8e\xd9\x9a\xd6\x61\xad\x69\x3b\x4d\x58\x33\x36\xdd\x57\x5c\xe1\xce\xf6\xd6\x74\xea\x0d\xbc\xc6\x9a\x22\xdc\xe4\xaf\xb3\x1d\x8e\x4d\xc7\xb1\x3b\xdd\xed\x70\xb3\x4f\x6b\xa0\xd6\xc6\xdb\xe1\x66\x8f\xd6\x74\xed\x96\xeb\x9a\xef\x37\x45\xb8\x79\x91\x35\xf3\xe3\x87\xab\x9c\x7d\xd6\x88\xc2\x58\x2e\x33\xfc\x0a\x87\xf6\x96\x5c\xd8\x84\x69\xe7\x2a\xe1\x13\x0e\x00\xa9\x21\x95\xe8\x16\x7c\x7f\xb9\x4c\x28\x78\x08\x42\x51\x90\xb9\x1e\x10\xb2\xe4\x10\x87\x71\x03\xee\x50\x70\xc6\x07\x02\x7b\x95\x51\x3d\x3f\x48\x6a\x9a\x14\x0a\xb0\x78\x26\x7c\xcc\xf2\xf9\x8c\xab\x38\xff\xab\x36\x60\xae\x67\xd0\x80\x8e\x40\x6a\xa9\x81\x30\x43\x7c\x04\x42\x0f\x82\x96\x28\x89\xf0\x40\x01\x28\x41\x5d\x89\xc6\x54\x0d\xe7\x27\x9e\xe0\x01\x7a\x4a\x84\xd5\xd3\x0d\x7a\x0a\x92\x5f\x84\xca\x67\xca\xf5\x91\x1c\xe3\xf0\x09\x61\x46\xd0\x13\x03\xf5\x84\x02\xc0\x32\x12\x10\x00\x53\xf2\x2d\xfa\x02\x7a\x1c\x00\x7d\xff\x0a\x3e\x68\x77\xd0\x07\xee\x46\x3a\x33\x16\x42\x0f\xd7\x43\xa5\x42\xd9\xab\x56\x07\x54\x0d\xa3\xfe\x5b\x97\x07\x55\xca\x3c\x3f\x9a\x10\xac\x70\x55\xcd\x6a\x55\x95\x00\xa8\x06\x58\x2a\x10\xd5\xd0\x8f\x06\x94\xc9\x2a\x65\x61\xa4\x64\x35\xb1\xed\x4d\x2c\xa9\x86\xe0\x87\xc8\xe5\xcc\xa3\x83\x28\x16\x56\x6a\x08\x12\xd0\xac\xc6\x5b\xab\x10\x1d\x19\xd4\xae\x2a\x17\x33\x08\x4c\xfa\x26\x27\x15\xd1\xb7\x50\xd1\x00\xf6\x85\x01\x49\xd9\xc0\x87\x8a\x54\x58\xe5\x8b\xfc\x3b\x02\x91\xf4\xf0\x7d\x29\x23\x7f\x69\x26\xed\xc1\x44\xe9\xf6\xf4\x7c\x5f\xf7\x23\xf7\x19\x54\x0f\xdd\x5b\x04\x3c\x1c\xf9\xea\xde\x7a\x73\xcf\x10\xfa\xef\x2f\x48\x60\x36\x80\x6b\xa9\xb0\x50\x3d\xa4\x1d\xfb\xa7\x4e\xf8\xa2\xcf\xd3\x22\x1e\xf5\x15\x88\x6b\x8f\xf5\xd0\xb5\x78\x83\xde\xfd\x82\xc4\xdb\xc7\x0c\x2c\xd0\xbb\x77\xe8\x7e\xa6\xd5\x17\x0d\x17\xd6\xf2\x28\xf8\x24\x29\x1f\xc5\xc3\xb8\x28\x3f\xa6\x8c\xf0\xf1\x75\x08\x82\x72\xd2\x43\xf6\x30\xcd\xf0\xb1\x54\xd7\xe9\xc9\x40\xf0\x28\xbc\x76\xb9\x1f\x05\x4c\xf6\xd0\xf7\x7b\xeb\x71\x84\xfd\x08\xee\xad\x1b\x74\x6f\x3d\x26\x4d\xc6\x87\xb1\x4b\xf3\x63\x1e\xde\x5b\x0f\x37\x28\xe0\x04\xf4\x30\xc0\xc4\x85\x30\x33\x0a\x01\x0e\x73\x96\xc6\xad\xa2\x2a\xea\xb4\x9c\x5a\xed\x06\x05\x20\x06\xf0\x37\x98\xf6\x90\x12\x11\xa4\xd5\xa6\xda\x9d\x6b\x8d\x0f\xdd\xa8\x36\x74\xd6\x64\xfa\x2f\x37\x85\x4b\x33\xad\x2f\x8a\xa2\x7c\xc9\x23\xe1\xc2\x6f\x1f\x74\x99\xa2\x7c\x20\x54\xfd\xce\x49\xdc\x06\x26\x23\xcc\x5c\x20\x45\xe5\x52\xd8\x16\xe5\xf5\x23\xea\x13\x10\xb7\xf1\x95\xb5\x02\xd9\x79\x21\x8d\x9a\x18\x6f\x29\x6e\xac\x87\xd5\xc6\xb4\x4b\x78\x50\x04\x4b\x13\x38\x67\xb5\x9e\x41\x33\x81\x95\x85\x52\x81\xad\xb3\xc2\xf1\x7c\x24\xb6\xcc\xa2\xc2\x87\xc2\xa2\x3f\x8b\x5b\xf8\xbf\x46\xc4\xc8\xdc\xa8\xfb\x04\xb8\xbb\xed\x7e\xc8\xe5\x46\xbe\x1b\x3a\x2d\x48\x2d\x9e\x28\x2f\x62\x71\x20\xfb\x82\xd9\x4a\xe1\xa4\x51\x6e\x6d\x6e\xc6\x4a\x5a\xbe\xd4\xf2\x79\xce\x6c\x2b\x14\xe0\xd1\x49\x01\x8c\x2d\x19\x79\xb3\x1c\x44\xf0\x54\xe6\xb3\x5d\xee\x73\xb1\xe1\xbd\x32\x21\x81\x3e\x96\xb0\xee\x7a\x8d\xef\xa8\x05\xf9\x43\x88\xad\xf8\x53\xad\x76\xdb\xbd\xbb\x5b\x77\x25\xfa\x58\x82\x28\x2a\x10\x4f\xb0\xd6\xb4\x5b\x8d\x0d\x01\x97\x06\xd8\xff\xec\x63\xb7\x80\x77\xb4\x5f\xf2\x23\xf3\xb8\xd0\xf7\x87\x1e\xf2\xb0\x2f\x21\xdf\xb7\x45\xe8\x80\xc6\x97\x78\x7d\x29\xe7\xa7\x41\x81\xac\x4c\xc2\x90\x8f\x3f\x71\x05\xff\x1a\x02\xfb\x18\x84\x6a\x9a\x76\xb4\x19\x31\x67\x83\x78\x33\x31\x9b\x42\xfd\xf9\xb8\xb2\xdb\xcf\xdf\x64\x49\xc8\xaf\x24\x64\xf6\xe8\x86\x91\x34\x12\x72\x72\xfa\x39\x3e\x3b\x24\x35\x17\x72\xec\xbe\x79\x75\xc6\x8f\x25\xad\xa6\x3f\xfb\xa7\xd5\x04\x7e\x25\xad\x6e\x66\xc6\x21\x68\x35\x9e\x8f\x92\x56\xd7\xd3\xaa\x0e\x07\x4e\x8d\x55\x33\x8b\xd1\x6b\x58\xd5\xb0\x64\x9d\x0f\x77\xff\xce\x31\x29\xb9\xf5\x95\xdc\xea\x73\x4c\xec\xad\xa8\x35\x00\xcc\x0e\x4b\xad\xba\xc7\x92\x5a\x8b\x11\x36\xab\x75\xa6\xd4\x1a\xa3\xef\x62\x99\x55\xe3\xfa\xb4\x98\xb5\x24\xd5\xf3\x23\xd5\xec\x33\x55\x33\xa9\x9a\x9e\xbc\xce\x87\xf5\x2b\x57\xd8\x47\xbf\x43\xc0\xc5\xb4\x64\xd5\x1c\xab\x06\xdb\x50\xaa\xd2\x23\xf9\xf2\x68\xd5\xbc\xe2\x6b\xd7\xea\xce\xf2\x2f\xd3\xf2\xef\x21\x23\xde\x72\x35\xd9\x84\xd3\x59\xad\x97\x72\x73\x70\x44\x62\x8e\x31\x7c\xb1\xc4\x7c\x82\x21\xef\x5f\x7f\x2d\xb9\xf9\xfc\xb8\x39\xbb\xc3\xc8\xcc\xcd\xa6\x7d\x48\xf3\x61\xfd\x40\xe5\x33\xfa\x26\xf1\x60\xe7\x0f\x77\x4f\x9b\x76\x09\x95\xcf\x5b\x3c\xb7\x95\x40\x1e\x43\x10\x2e\x30\x55\x46\xb4\x97\x1a\xd1\x6a\xcc\x1c\xef\x09\x6c\x06\x82\xbb\x35\xc2\x93\x31\x30\xce\x83\x3f\x0f\x1b\xd8\xe2\x49\x31\x39\x4c\x0a\xa1\x6b\xf5\x79\xc4\x92\xed\xd8\x96\x75\x83\xac\x22\xd0\x5a\x3e\xee\x83\x6f\xba\x3c\x8c\x7c\x8d\xd6\x06\xd3\x49\xe7\x9a\x82\x7b\xc8\xb2\xf3\xfb\xd1\x92\xba\x2e\xf6\x13\x1e\xa5\x0c\xb0\xc8\x0f\xe1\x0a\x74\xe2\x0d\x6b\xc7\x74\xf1\xcf\x87\xf0\xb1\xfe\x87\x9b\xc7\xb5\x52\x24\x25\x80\xc9\x34\x4f\xd6\x3e\x0c\x80\xc5\x6f\x0b\xe4\xeb\x0c\x80\x07\x69\x7f\x66\x9d\x98\xbf\x70\xf6\x25\x7a\x32\x1b\x99\xd7\x88\x1e\xc3\x76\xe7\xf9\x4d\xe4\xf6\xf3\xb7\x4b\xd4\x3c\x6e\x18\x6d\x23\x79\xf0\x00\x1e\x23\x09\xe2\xde\x42\x5c\x14\xe6\xa6\x4f\x04\x4c\xf9\x94\xf8\xb0\xbe\x47\x37\x8c\xe6\xb6\x55\xb6\x5f\xdd\x28\xe5\xd5\x72\xde\x59\xcb\x2b\x37\x8c\x8e\xa8\xae\x52\xb4\xeb\x1b\x7e\x16\xdd\x8b\x73\x8d\xe6\xdd\x1a\xa8\x3d\xde\x70\x64\x2a\x97\xbd\x6c\x52\xca\xbe\x52\xf6\x95\xb2\xef\x62\x65\x5f\xe6\x8d\xb1\x35\xb2\xcf\xf0\x5e\xd9\x21\x36\x77\x9c\xb6\xf0\x7b\xe1\xce\x8d\x55\x59\xa7\x33\x9a\xa6\x0c\xbb\x59\x6a\xb7\x4b\xd5\x6e\x27\xb1\xd9\xe3\x06\x25\x00\x4d\x0f\xec\x66\xa9\x97\x36\x33\xa3\xd4\x4b\x69\xdd\x63\xe8\xa5\x15\x42\x3a\x41\x37\x4b\xc9\x94\x7a\x73\x1e\x92\x29\xfb\x5a\xbb\x59\x32\x99\x5e\x7e\x9f\xdf\x47\x92\x4d\x3b\x97\xb8\x58\xb6\xd5\xbe\x9c\xf2\xf9\x60\x29\x82\x8e\xbb\xab\x66\x7f\x8f\x07\x4b\xd5\x53\x6c\x76\xa9\x7a\x8e\xe7\x62\xb9\x4a\xb4\x3f\xc9\xa3\xeb\x56\x42\x3f\x92\x95\x35\x7b\x6a\xcd\x2a\xe8\x8f\xb1\x77\x2c\x5f\xd6\x68\xbc\x5b\xf7\xba\xf5\x3a\xf1\x2a\xad\x1a\xb6\x2b\x8e\xd3\xb6\x2b\x5d\xdb\x73\x2b\xae\xdd\x72\xea\x6d\x68\x7b\xae\xd7\x5f\xe7\x5c\x32\x65\x6b\xbc\xeb\xdc\x75\xde\xdf\x39\xeb\xbc\xfb\xc0\xd1\x7b\x46\x04\xa7\x44\xa2\x0f\x02\x70\x80\xb8\x87\x3e\xfa\xe0\x2a\x41\x5d\xf4\x65\x08\x10\xfe\x65\x3f\xce\x37\x1a\x35\xaf\xed\x01\x54\x88\xe3\x40\xc5\xc1\x76\xb3\xd2\xaf\x93\x56\xa5\xde\x68\xd4\xec\x76\x07\xdc\x7a\xad\xf1\x2a\xe7\xdf\x37\x6d\xc7\x3e\x51\xe7\xa1\xe1\xb6\x1b\xd0\x6f\x54\x5a\xad\x26\xae\x38\xb6\xd3\xaf\x60\x8f\x90\x8a\xdd\xea\xb4\xdc\x6e\xbf\xdb\xaa\x93\xee\xab\x9c\xbf\x73\x6e\xef\x1a\xf6\x41\x9d\x5f\xcb\x77\x9b\x6c\x26\xcd\xdf\x98\x77\xba\xcf\xd2\x5e\x7b\xff\xda\x53\x20\x95\xfd\x22\x97\x39\x90\x32\x7d\xb7\x6b\x79\x9f\xe5\x6f\xff\xb8\xac\x18\x2a\xf9\xc0\xd2\xe6\x61\x94\x00\x4c\x1e\xfb\x53\x05\xb2\x68\x8d\x79\x2c\xa8\x82\x34\x3b\x6d\x92\x80\xa0\x23\xac\xe8\x08\xae\x23\x46\xd5\x72\x94\x75\x83\x18\x67\x9f\x60\x10\xe7\xcf\x66\xbd\x38\x2a\x5a\x34\x53\xc6\x46\xc5\xe8\x99\xd5\x7a\xcd\xde\x49\xca\x8f\x16\x1e\x2d\x90\xa5\x35\x5b\x06\x49\x17\x1b\x2b\x2d\x10\x5f\x46\x4c\x67\x12\x31\xfd\x9a\xe0\xf7\xb4\xfd\xbc\xd0\xa8\xe9\x6c\x17\x8a\xb3\xdf\xf8\x5c\xa3\x6f\x0c\x5f\x02\x5d\x7c\x2c\x12\xd4\x98\x8b\xe7\xcb\xd2\x37\x0c\xd4\xe6\xe2\x26\xa6\x9b\x47\x01\xee\xa8\x48\xdc\x24\xb9\x72\x69\xfd\xb8\xd4\x36\xf9\x42\xa7\xa9\x6d\x18\xec\x78\xc9\x75\x0b\x61\xb3\x40\x95\xbe\xad\x2e\x50\x54\xea\x9a\x52\xd7\x94\xba\xa6\xd4\x35\x97\xab\x6b\x32\x5f\x0b\x5f\xa3\x6b\x0c\xdf\x14\x9f\xdf\x4e\x3e\x0b\xee\x82\x94\xb0\xf3\x4f\x2d\x9e\xb6\xb2\x09\x53\xb7\xb7\x58\xbc\x89\x18\xa3\x6c\x50\x28\x6e\x7c\xee\x3e\x03\x29\xca\x4a\x5e\x11\x29\x78\x81\x84\x3d\x33\x3e\x66\xdb\x3d\x4c\xc7\x93\x03\x3f\x4b\xc7\x93\x52\x52\x15\x43\x76\x56\xeb\xa5\x92\x6a\x8e\xbf\xe3\xad\x18\x25\x70\x8e\x55\x55\x02\x5f\x7d\x18\xbf\x03\x72\x83\x52\x78\x5e\xac\xca\x0a\xf0\xa4\x94\x57\x67\x22\xaf\x4e\xdd\xc5\x52\x59\xa5\xde\x9c\x87\xb2\xca\xfe\xe5\x13\xb3\xb2\x32\xfd\x7d\x94\xc5\xdb\x18\x63\x1c\x5e\x96\xa8\x92\x63\x1c\x6e\xfd\xad\xaf\xa2\x77\x6b\xb5\x94\x2a\x37\x19\x5e\xa6\x32\xd2\x20\x3a\xf2\xb7\xbb\xe2\x37\x62\x81\x5c\xae\xf8\x29\xb7\x19\x96\xea\xa7\x54\x3f\x97\xa1\x7e\xae\xb2\xbf\xf5\xff\x0f\x57\x3f\xaf\xfe\x17\x00\x00\xff\xff\x15\x36\x15\xa1\xb1\x75\x00\x00")

func systemJsonBytes() ([]byte, error) {
	return bindataRead(
		_systemJson,
		"system.json",
	)
}

func systemJson() (*asset, error) {
	bytes, err := systemJsonBytes()
	if err != nil {
		return nil, err
	}

	info := bindataFileInfo{name: "system.json", size: 30129, mode: os.FileMode(420), modTime: time.Unix(1548273842, 0)}
	a := &asset{bytes: bytes, info: info, digest: [32]uint8{0xab, 0x8a, 0xdc, 0xca, 0xfb, 0x78, 0xd6, 0x6c, 0xf1, 0xd6, 0x9a, 0x91, 0xd2, 0xf8, 0x27, 0xee, 0x75, 0x30, 0xe3, 0x31, 0xa9, 0x6f, 0x2d, 0x68, 0xef, 0xf0, 0x8, 0xd6, 0x41, 0x81, 0x83, 0x87}}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	canonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[canonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// AssetString returns the asset contents as a string (instead of a []byte).
func AssetString(name string) (string, error) {
	data, err := Asset(name)
	return string(data), err
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if err != nil {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// MustAssetString is like AssetString but panics when Asset would return an
// error. It simplifies safe initialization of global variables.
func MustAssetString(name string) string {
	return string(MustAsset(name))
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	canonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[canonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetDigest returns the digest of the file with the given name. It returns an
// error if the asset could not be found or the digest could not be loaded.
func AssetDigest(name string) ([sha256.Size]byte, error) {
	canonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[canonicalName]; ok {
		a, err := f()
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("AssetDigest %s can't read by error: %v", name, err)
		}
		return a.digest, nil
	}
	return [sha256.Size]byte{}, fmt.Errorf("AssetDigest %s not found", name)
}

// Digests returns a map of all known files and their checksums.
func Digests() (map[string][sha256.Size]byte, error) {
	mp := make(map[string][sha256.Size]byte, len(_bindata))
	for name := range _bindata {
		a, err := _bindata[name]()
		if err != nil {
			return nil, err
		}
		mp[name] = a.digest
	}
	return mp, nil
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"system.json": systemJson,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"},
// AssetDir("data/img") would return []string{"a.png", "b.png"},
// AssetDir("foo.txt") and AssetDir("notexist") would return an error, and
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		canonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(canonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		rv = append(rv, childName)
	}
	return rv, nil
}

type bintree struct {
	Func     func() (*asset, error)
	Children map[string]*bintree
}

var _bintree = &bintree{nil, map[string]*bintree{
	"system.json": &bintree{systemJson, map[string]*bintree{}},
}}

// RestoreAsset restores an asset under the given directory.
func RestoreAsset(dir, name string) error {
	data, err := Asset(name)
	if err != nil {
		return err
	}
	info, err := AssetInfo(name)
	if err != nil {
		return err
	}
	err = os.MkdirAll(_filePath(dir, filepath.Dir(name)), os.FileMode(0755))
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
	if err != nil {
		return err
	}
	return os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
}

// RestoreAssets restores an asset under the given directory recursively.
func RestoreAssets(dir, name string) error {
	children, err := AssetDir(name)
	// File
	if err != nil {
		return RestoreAsset(dir, name)
	}
	// Dir
	for _, child := range children {
		err = RestoreAssets(dir, filepath.Join(name, child))
		if err != nil {
			return err
		}
	}
	return nil
}

func _filePath(dir, name string) string {
	canonicalName := strings.Replace(name, "\\", "/", -1)
	return filepath.Join(append([]string{dir}, strings.Split(canonicalName, "/")...)...)
}

//lint:file-ignore ST1005 Ignore error strings should not be capitalized