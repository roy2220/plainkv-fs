package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/roy2220/plainkv"
)

type fs1 struct{}

func (fs1) Root() (fs.Node, error) {
	return makeDir("/"), nil
}

type dir struct {
	object
}

func makeDir(fileName string) dir {
	return dir{object: object{fileName}}
}

func (d dir) Attr(_ context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | (0777 &^ umask)
	attr.Nlink = 2
	rawName := []byte(d.Name)
	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		return syscall.ENOENT
	}

	if key, _ := it.ReadKeyAll(); string(key) != d.Name {
		return syscall.ENOTDIR
	}

	return nil
}

func (d dir) Open(_ context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	rawName := []byte(d.Name)
	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		return nil, syscall.ENOENT
	}

	if key, _ := it.ReadKeyAll(); string(key) != d.Name {
		return nil, syscall.ENOTDIR
	}

	dirHandle := d.AddRef()
	return dirHandle, nil
}

func (d dir) Lookup(_ context.Context, name string) (fs.Node, error) {
	name = d.Name + name + "/"
	rawName := []byte(name)
	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		return nil, syscall.ENOENT
	}

	var node fs.Node

	if key, _ := it.ReadKeyAll(); string(key) == name {
		node = makeDir(name)
	} else {
		node = makeFile(name[:len(name)-1])
	}

	return node, nil
}

func (d dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	name := d.Name + req.Name + "/"
	rawName := []byte(name)
	orderedDictLock.Lock()
	defer orderedDictLock.Unlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if !it.IsAtEnd() {
		return nil, syscall.EEXIST
	}

	orderedDict.Set(rawName, nil, false)
	return makeDir(name), nil
}

func (d dir) Create(_ context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	name := d.Name + req.Name + "/"
	rawName := []byte(name)
	orderedDictLock.Lock()
	defer orderedDictLock.Unlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if !it.IsAtEnd() {
		return nil, nil, syscall.EEXIST
	}

	orderedDict.Set(rawName[:len(rawName)-1], nil, false)
	file := makeFile(name[:len(name)-1])
	fileHandle := file.AddRef()
	return file, fileHandle, nil
}

func (d dir) Remove(_ context.Context, req *fuse.RemoveRequest) error {
	name := d.Name + req.Name + "/"
	rawName := []byte(name)
	orderedDictLock.Lock()
	defer orderedDictLock.Unlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		return syscall.ENOENT
	}

	if req.Dir {
		if key, _ := it.ReadKeyAll(); string(key) != name {
			return syscall.ENOTDIR
		}

		if makeDir(name).HasRefs() {
			return syscall.EBUSY
		}

		it = orderedDict.RangeAsc([]byte(string(rawName)+string('\x00')), plainkv.MaxKey)

		if key, err := it.ReadKeyAll(); err == nil && bytes.HasPrefix(key, rawName) {
			return syscall.ENOTEMPTY
		}

		orderedDict.Clear(rawName, false)
	} else {
		if key, _ := it.ReadKeyAll(); string(key) == name {
			return syscall.EISDIR
		}

		if makeDir(name[:len(name)-1]).HasRefs() {
			return syscall.EBUSY
		}

		orderedDict.Clear(rawName[:len(rawName)-1], false)
	}

	return nil
}

func (d dir) AddRef() *dirHandle {
	return (*dirHandle)(d.object.AddRef())
}

type dirHandle objectHandle

func (dh *dirHandle) Release(_ context.Context, req *fuse.ReleaseRequest) error {
	makeDir(string(dh.RawName)).RemoveRef()
	return nil
}

func (dh *dirHandle) ReadDirAll(_ context.Context) ([]fuse.Dirent, error) {
	dirents := make([]fuse.Dirent, 2)

	dirents[0] = fuse.Dirent{
		Name: ".",
		Type: fuse.DT_Dir,
	}

	dirents[1] = fuse.Dirent{
		Name: "..",
		Type: fuse.DT_Dir,
	}

	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()

	for it := orderedDict.RangeAsc([]byte(string(dh.RawName)+string('\x00')), plainkv.MaxKey); !it.IsAtEnd(); {
		key, _ := it.ReadKeyAll()

		if !bytes.HasPrefix(key, dh.RawName) {
			break
		}

		if key[len(key)-1] == '/' {
			dirents = append(dirents, fuse.Dirent{
				Name: string(key[len(dh.RawName) : len(key)-1]),
				Type: fuse.DT_Dir,
			})

			key[len(key)-1]++
			it = orderedDict.RangeAsc(key, plainkv.MaxKey)
		} else {
			dirents = append(dirents, fuse.Dirent{
				Name: string(key[len(dh.RawName):]),
				Type: fuse.DT_File,
			})

			it.Advance()
		}
	}

	return dirents, nil
}

type file struct {
	object
}

func makeFile(fileName string) file {
	return file{object: object{fileName}}
}

func (f file) Attr(_ context.Context, attr *fuse.Attr) error {
	attr.Mode = 0666 &^ umask
	attr.Nlink = 1
	name := f.Name + "/"
	rawName := []byte(name)
	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()
	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		return syscall.ENOENT
	}

	if key, _ := it.ReadKeyAll(); string(key) == name {
		return syscall.EISDIR
	}

	valueSize, _ := it.GetValueSize()
	attr.Size = uint64(valueSize)
	return nil
}

func (f file) Open(_ context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	name := f.Name + "/"
	rawName := []byte(name)

	if req.Flags&fuse.OpenCreate == 0 && req.Flags&fuse.OpenTruncate == 0 {
		orderedDictLock.RLock()
		defer orderedDictLock.RUnlock()
	} else {
		orderedDictLock.Lock()
		defer orderedDictLock.Unlock()
	}

	it := orderedDict.RangeAsc(rawName[:len(rawName)-1], rawName)

	if it.IsAtEnd() {
		if req.Flags&fuse.OpenCreate == 0 {
			return nil, syscall.ENOENT
		}

		orderedDict.Set(rawName[:len(rawName)-1], nil, false)
		fileHandle := f.AddRef()
		return fileHandle, nil
	}

	if key, _ := it.ReadKeyAll(); string(key) == name {
		return nil, syscall.EISDIR
	}

	if req.Flags&fuse.OpenTruncate != 0 {
		orderedDict.Set(rawName[:len(rawName)-1], nil, false)
	}

	fileHandle := f.AddRef()
	return fileHandle, nil
}

func (f file) AddRef() *fileHandle {
	return (*fileHandle)(f.object.AddRef())
}

type fileHandle objectHandle

func (fh *fileHandle) Release(_ context.Context, req *fuse.ReleaseRequest) error {
	makeFile(string(fh.RawName)).RemoveRef()
	return nil
}

func (fh *fileHandle) Read(_ context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buffer := make([]byte, req.Size)
	orderedDictLock.RLock()
	defer orderedDictLock.RUnlock()
	numberOfBytes, _ := orderedDict.RangeAsc(fh.RawName, fh.RawName).ReadValue(int(req.Offset), buffer)
	resp.Data = buffer[:numberOfBytes]
	return nil
}

func (fh *fileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	orderedDictLock.Lock()
	defer orderedDictLock.Unlock()
	data, _ := orderedDict.RangeAsc(fh.RawName, fh.RawName).ReadValueAll()
	i := int(req.Offset)
	j := i + len(req.Data)

	if j > len(data) {
		buffer := make([]byte, j)
		copy(buffer, data)
		data = buffer
	}

	resp.Size = copy(data[i:j], req.Data)
	orderedDict.Set(fh.RawName, data, false)
	return nil
}

type object struct {
	Name string
}

func (o object) AddRef() *objectHandle {
	objectHandlesLock.Lock()
	defer objectHandlesLock.Unlock()
	objectHandle1, ok := objectHandles[o.Name]

	if ok {
		objectHandle1.refCount++
	} else {
		objectHandle1 = &objectHandle{[]byte(o.Name), 1}
		objectHandles[o.Name] = objectHandle1
	}

	return objectHandle1
}

func (o object) RemoveRef() bool {
	objectHandlesLock.Lock()
	defer objectHandlesLock.Unlock()
	objectHandle1, ok := objectHandles[o.Name]

	if !ok {
		return false
	}

	if objectHandle1.refCount == 1 {
		delete(objectHandles, o.Name)
	} else {
		objectHandle1.refCount--
	}

	return true
}

func (o object) HasRefs() bool {
	objectHandlesLock.RLock()
	defer objectHandlesLock.RUnlock()
	_, ok := objectHandles[o.Name]
	return ok
}

type objectHandle struct {
	RawName []byte

	refCount int64
}

var (
	orderedDict       *plainkv.OrderedDict
	orderedDictLock   sync.RWMutex
	objectHandles     map[string]*objectHandle
	objectHandlesLock sync.RWMutex
	umask             os.FileMode
)

func getUmask() os.FileMode {
	temp := syscall.Umask(0)
	syscall.Umask(temp)
	return os.FileMode(temp)
}

func main() {
	flag.Parse()

	if flag.NArg() != 2 {
		fmt.Printf("usage: %s <db file> <mount point>", os.Args[0])
		os.Exit(2)
	}

	dbFileName := flag.Arg(0)
	mountPointName := flag.Arg(1)
	var err error
	orderedDict, err = plainkv.OpenOrderedDict(dbFileName, true)

	if err != nil {
		panic(err)
	}

	defer orderedDict.Close()
	orderedDict.SetIfNotExists([]byte{'/'}, nil, false)
	objectHandles = map[string]*objectHandle{}
	umask = getUmask()

	conn, err := fuse.Mount(
		mountPointName,
		fuse.FSName("plainkv-fs"),
		fuse.Subtype("plainkv"),
		fuse.LocalVolume(),
		fuse.VolumeName("plainkv-db"),
	)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	if err := fs.Serve(conn, fs1{}); err != nil {
		panic(err)
	}

	<-conn.Ready

	if err := conn.MountError; err != nil {
		panic(err)
	}
}