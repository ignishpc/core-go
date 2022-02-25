package impl

import (
	"bufio"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"os"
	"strconv"
	"strings"
)

type ICacheImpl struct {
	IBaseImpl
	nextContextId int64
	context       map[int64]storage.IPartitionGroupBase
	cache         map[int64]storage.IPartitionGroupBase
}

func NewICacheImpl(executorData *core.IExecutorData) *ICacheImpl {
	return &ICacheImpl{
		IBaseImpl:     IBaseImpl{executorData},
		nextContextId: 11,
	}
}

func (this *ICacheImpl) fileCache() (string, error) {
	dir, err := this.executorData.InfoDirectory()
	if err != nil {
		return "", ierror.Raise(err)
	}

	return dir + "/cache" + strconv.Itoa(this.executorData.GetContext().ExecutorId()) + ".bak", nil
}

func (this *ICacheImpl) SaveContext() (int64, error) {
	var id int64
	if len(this.context) <= 10 {
		for i := int64(0); i <= 10; i++ {
			if _, used := this.context[i]; !used {
				id = i
			}
		}
	} else {
		id = this.nextContextId
		this.nextContextId++
	}
	logger.Info("CacheContext: saving context ", id)
	this.context[id] = this.executorData.GetPartitionsAny()
	return id, nil
}

func (this *ICacheImpl) ClearContext() error {
	this.executorData.DeletePartitions()
	this.executorData.ClearVariables()
	vars := this.executorData.GetContext().Vars()
	for k := range vars {
		delete(vars, k)
	}
	return nil
}

func (this *ICacheImpl) LoadContext(id int64) error {
	value, present := this.context[id]
	if present && value == this.executorData.GetPartitionsAny() {
		delete(this.context, id)
		return nil
	}
	logger.Info("CacheContext: loading context ", id)

	if !present {
		return ierror.RaiseMsg("context " + strconv.FormatInt(id, 10) + " not found")
	}
	this.executorData.SetPartitionsAny(value)
	this.executorData.ClearVariables()
	delete(this.context, id)
	return nil
}

func (this *ICacheImpl) LoadContextAsVariable(id int64, name string) error {
	value, present := this.context[id]
	logger.Info("CacheContext: loading context " + strconv.FormatInt(id, 10) + " as variable " + name)

	if !present {
		return ierror.RaiseMsg("context " + strconv.FormatInt(id, 10) + " not found")
	}
	core.SetVariable[storage.IPartitionGroupBase](this.executorData, name, value)
	delete(this.context, id)
	return nil
}

func (this *ICacheImpl) Cache(id int64, level int8) error {
	if level == 0 { // NO_CACHE
		value, present := this.cache[id]
		if !present {
			logger.Warn("CacheContext: removing non existent cache " + strconv.FormatInt(id, 10))
			return nil
		}
		delete(this.cache, id)
		if this.executorData.GetPartitionTools().IsDiskGroup(value) {
			cache, err := this.fileCache()
			if err != nil {
				return ierror.Raise(err)
			}
			found := false
			var lines []string

			file, err := os.Open(cache)
			if err != nil {
				return ierror.Raise(err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if !found || strings.HasPrefix(line, strconv.FormatInt(id, 10)+"\x00") {
					found = true
					continue
				}
				lines = append(lines, line)
			}
			if err = scanner.Err(); err != nil {
				return ierror.Raise(err)
			}
			_, err = file.Seek(0, 0)
			if err != nil {
				return ierror.Raise(err)
			}
			err = file.Truncate(0)
			if err != nil {
				return ierror.Raise(err)
			}
			for _, line := range lines {
				if _, err = file.WriteString(line + "\n"); err != nil {
					return ierror.Raise(err)
				}
			}
		}
	}

	groupCache := this.executorData.GetPartitionsAny()
	sameLevel := true

	if level == 1 { // PRESERVE
		if this.executorData.GetPartitionTools().IsDiskGroup(groupCache) {
			level = 4
		} else if this.executorData.GetPartitionTools().IsRawMemoryGroup(groupCache) {
			level = 3
		} else {
			level = 2
		}
	}

	if level == 2 { // MEMORY
		logger.Info("CacheContext: saving partition in " + storage.IMemoryPartitionType + " cache")
		if !this.executorData.GetPartitionTools().IsMemoryGroup(groupCache) {
			sameLevel = false
			group := groupCache.NewGroup()
			for i := 0; i < groupCache.Size(); i++ {
				group.AddMemoryPartition(0)
				if err := groupCache.GetBase(i).CopyTo(group.GetBase(i)); err != nil {
					return ierror.Raise(err)
				}
			}
			groupCache = group
		}
	} else if level == 3 { // RAW_MEMORY
		logger.Info("CacheContext: saving partition in " + storage.IRawMemoryPartitionType + " cache")
		return ierror.RaiseMsg("Not implemented yet") //TODO

	} else if level == 4 { // DISK
		logger.Info("CacheContext: saving partition in " + storage.IDiskPartitionType + " cache")
		return ierror.RaiseMsg("Not implemented yet") //TODO
	}

	if sameLevel {
		groupCache.SetCache(true)
	}

	this.cache[id] = groupCache
	return nil
}

func (this *ICacheImpl) LoadCacheFromDisk() error {
	cache, err := this.fileCache()
	if err != nil {
		return ierror.Raise(err)
	}
	if _, err := os.Stat("./conf/app.ini"); os.IsNotExist(err) {
		logger.Info("CacheContext: cache file found, loading")
		file, err := os.Open(cache)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fileds := strings.Split(scanner.Text(), "\x00")
			id, err := strconv.ParseInt(fileds[0], 10, 0)
			if err != nil {
				ierror.Raise(err)
			}
			group := storage.NewIPartitionGroup[any]()
			for _, path := range fileds[2:] {
				part, err := storage.NewIDiskPartition[any](path, 0, false, true, true)
				if err != nil {
					return ierror.Raise(err)
				}
				group.Add(part)
			}
			this.cache[id] = group
		}
	}

	return nil
}

func (this *ICacheImpl) LoadCache(id int64) error {
	logger.Info("CacheContext: loading partition from cache")
	if value, present := this.cache[id]; present {
		this.executorData.SetPartitionsAny(value)
		return nil
	}
	return ierror.RaiseMsg("cache " + strconv.FormatInt(id, 10) + " not found")
}
