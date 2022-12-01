package impl

import (
	"bufio"
	"bytes"
	"errors"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	iio "ignis/executor/core/iio"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
	"io"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
)

type IIOImpl struct {
	IBaseImpl
}

func NewIIOImpl(executorData *core.IExecutorData) *IIOImpl {
	return &IIOImpl{
		IBaseImpl{executorData},
	}
}

func PartitionApproxSize[T any](this *IIOImpl) (int64, error) {
	logger.Info("IO: calculating partition size")
	group, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	if group.Size() == 0 {
		return 0, nil
	}

	size := int64(0)
	if this.executorData.GetPartitionTools().IsMemoryGroup(group) {
		for _, part := range group.Iter() {
			size += part.Size()
		}
		if iio.IsContiguous[T]() {
			size *= int64(utils.TypeObj[T]().Size())
		} else {
			if eSize, err := this.executorData.GetProperties().TransportElemSize(); err != nil {
				return 0, ierror.Raise(err)
			} else {
				size *= eSize
			}
		}
	} else {
		for _, part := range group.Iter() {
			size += part.Bytes()
		}
	}

	return size, nil
}

func (this *IIOImpl) PlainFile(path string, minPartitions int64, delim string) error {
	logger.Info("IO: reading plain file")
	return this.plainOrTextFile(path, minPartitions, delim)
}

func (this *IIOImpl) TextFile(path string, minPartitions int64) error {
	logger.Info("IO: reading text file")
	return this.plainOrTextFile(path, minPartitions, "\n")
}

func readBytes(reader *bufio.Reader, buffer *[]byte, delim []byte) ([]byte, error) {
	if len(delim) == 1 {
		return reader.ReadBytes(delim[0])
	}
	*buffer = (*buffer)[:0]
	var line []byte
	var err error
	dsize := len(delim)
	for err != io.EOF {
		line, err = reader.ReadBytes(delim[dsize-1])
		if err != nil && err != io.EOF {
			return nil, ierror.Raise(err)
		}
		*buffer = append(*buffer, line...)
		if len(*buffer) > dsize && bytes.Compare((*buffer)[len(*buffer)-dsize:], delim) == 0 {
			break
		}
	}
	return (*buffer)[:], err
}

func (this *IIOImpl) plainOrTextFile(path string, minPartitions int64, delim string) error {
	size := int64(0)
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return ierror.Raise(err)
	} else {
		size = info.Size()
	}
	logger.Info("IO: file has ", size, " Bytes")
	result, err := core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}
	threadGroup := make([]*storage.IPartitionGroup[string], ioCores)
	elements := int64(0)

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		file, err := this.openFileRead(path)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()
		id := rctx.ThreadId()
		globalThreadId := this.executorData.GetContext().ExecutorId()*ioCores + id
		threads := this.executorData.GetContext().Executors() * ioCores
		exChunk := size / int64(threads)
		exChunkInit := int64(globalThreadId) * exChunk
		exChunkEnd := exChunkInit + exChunk
		minPartitionSize, err := this.executorData.GetProperties().PartitionMinimal()
		if err != nil {
			return ierror.Raise(err)
		}
		minPartitions := int64(math.Ceil(float64(minPartitions) / float64(threads)))
		buffer := make([]byte, 0, 1024)
		dsize := len(delim)
		bdelim := []byte(delim)

		if globalThreadId > 0 {
			padding := utils.Ternary(exChunkInit >= int64(dsize), exChunkInit-int64(dsize), exChunkInit)
			if _, err = file.Seek(padding, 0); err != nil {
				return ierror.Raise(err)
			}
			value := make([]byte, 256)
			if dsize == 1 {
			OUT:
				for true {
					if n, err := file.Read(value); err != nil && err != io.EOF {
						return ierror.Raise(err)
					} else if n == 0 {
						break
					} else {
						for i := 0; i < n; i++ {
							if value[i] == delim[0] {
								break OUT
							}
							exChunkInit++
						}
					}
				}
			} else {
				reader := bufio.NewReader(file)
				if chunk, err := readBytes(reader, &buffer, bdelim); err != nil {
					return ierror.Raise(err)
				} else {
					exChunkInit = padding + int64(len(chunk))
				}
			}
			if _, err = file.Seek(exChunkInit, 0); err != nil {
				return ierror.Raise(err)
			}
			if globalThreadId == threads-1 {
				exChunkEnd = size
			}
		}

		if exChunk/minPartitionSize < minPartitions {
			minPartitionSize = exChunk / minPartitions
		}

		if threadGroup[id], err = core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools()); err != nil {
			return ierror.Raise(err)
		}
		partition, err := core.NewPartitionDef[string](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		writeIterator, err := partition.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		threadGroup[id].Add(partition)
		threadElements := int64(0)
		partitionInit := exChunkInit
		filepos := exChunkInit
		reader := bufio.NewReaderSize(file, utils.Min(1*10e9, utils.Max(65*1024, int(minPartitionSize/(4*int64(ioCores))))))
		for filepos < exChunkEnd {
			if (filepos - partitionInit) > minPartitionSize {
				if err = partition.Fit(); err != nil {
					return ierror.Raise(err)
				}
				if partition, err = core.NewPartitionDef[string](this.executorData.GetPartitionTools()); err != nil {
					return ierror.Raise(err)
				}
				if writeIterator, err = partition.WriteIterator(); err != nil {
					return ierror.Raise(err)
				}
				threadGroup[id].Add(partition)
				partitionInit = filepos
			}
			line, err := readBytes(reader, &buffer, bdelim)
			eof := err == io.EOF
			if err != nil && err != io.EOF {
				return ierror.Raise(err)
			}
			filepos += int64(len(line))
			threadElements++
			if eof {
				if err := writeIterator.Write(string(line)); err != nil {
					return ierror.Raise(err)
				}
				break
			} else {
				if err := writeIterator.Write(string(line[:len(line)-dsize])); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return rctx.Critical(func() error {
			elements += threadElements
			return nil
		})
	}); err != nil {
		return err
	}

	for _, group := range threadGroup {
		for _, part := range group.Iter() {
			result.Add(part)
		}
	}

	logger.Info("IO: created ", result.Size(), " partitions, ", elements, " lines and ", size, " Bytes read")
	core.SetPartitions[string](this.executorData, result)
	return nil
}

func PartitionObjectFile[T any](this *IIOImpl, path string, first int64, partitions int64) error {
	logger.Info("IO: reading partition object file")
	group, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(int(partitions), func(p int) error {
			fileName, err := this.partitionFileName(path, first+int64(p))
			if err != nil {
				return ierror.Raise(err)
			}
			file, err := this.openFileRead(fileName) //Only to check
			if err != nil {
				return ierror.Raise(err)
			}
			_ = file.Close()
			open, err := storage.NewIDiskPartition[T](fileName, 0, true, true, true)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = group.Get(p).CopyFrom(open); err != nil {
				return ierror.Raise(err)
			}
			return ierror.Raise(group.Get(p).Fit())
		})
	}); err != nil {
		return err
	}

	core.SetPartitions[T](this.executorData, group)
	return nil
}

func (this *IIOImpl) PartitionTextFile(path string, first int64, partitions int64) error {
	logger.Info("IO; reading partitions text file")
	group, err := core.NewPartitionGroupWithSize[string](this.executorData.GetPartitionTools(), int(partitions))
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(int(partitions), func(p int) error {
			fileName, err := this.partitionFileName(path, first+int64(p))
			if err != nil {
				return ierror.Raise(err)
			}
			file, err := this.openFileRead(fileName)
			if err != nil {
				return ierror.Raise(err)
			}
			partition := group.Get(p)
			writeIterator, err := partition.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			reader := bufio.NewReader(file)
			for true {
				line, err := reader.ReadBytes('\n')
				eof := err == io.EOF
				if err != nil && err != io.EOF {
					return ierror.Raise(err)
				}
				if eof {
					if len(line) == 0 {
						break
					}
					if err = writeIterator.Write(string(line)); err != nil {
						return ierror.Raise(err)
					}
					return nil
				} else {
					if err = writeIterator.Write(string(line[:len(line)-1])); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}

	core.SetPartitions[string](this.executorData, group)
	return nil
}

func PartitionJsonFile[T any](this *IIOImpl, path string, first int64, partitions int64) error {
	logger.Info("IO: reading partition json file")
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func SaveAsObjectFile[T any](this *IIOImpl, path string, compression int8, first int64) error {
	logger.Info("IO: saving as object file")
	group, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			var fileName string
			if err := rctx.Critical(func() error {
				fileName, err = this.partitionFileName(path, first+int64(p))
				if err != nil {
					return ierror.Raise(err)
				}
				file, err := this.openFileWrite(fileName) //Only to check
				if err != nil {
					return ierror.Raise(err)
				}
				_ = file.Close()
				return err
			}); err != nil {
				return ierror.Raise(err)
			}

			save, err := storage.NewIDiskPartition[T](fileName, 0, true, true, false)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = group.Get(p).CopyTo(save); err != nil {
				return ierror.Raise(err)
			}
			if err = save.Sync(); err != nil {
				return ierror.Raise(err)
			}
			group.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return err
	}

	return nil
}

func SaveAsTextFile[T any](this *IIOImpl, path string, first int64) error {
	logger.Info("IO: saving as text file")
	group, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	isMemory := this.executorData.GetPartitionTools().IsMemoryGroup(group)

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			var fileName string
			var file *os.File
			if err := rctx.Critical(func() error {
				fileName, err = this.partitionFileName(path, first+int64(p))
				if err != nil {
					return ierror.Raise(err)
				}
				file, err = this.openFileWrite(fileName)
				if err != nil {
					return ierror.Raise(err)
				}
				return err
			}); err != nil {
				return ierror.Raise(err)
			}
			defer file.Close()

			if isMemory {
				list := group.Get(p).Inner().(storage.IList)
				if err = iio.Print(file, list.Array()); err != nil {
					return ierror.Raise(err)
				}
			} else {
				it, err := group.Get(p).ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = iio.Print(file, it); err != nil {
					return ierror.Raise(err)
				}
			}

			group.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return err
	}

	return nil
}

func SaveAsJsonFile[T any](this *IIOImpl, path string, first int64, pretty bool) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IIOImpl) partitionFileName(path string, index int64) (string, error) {
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return "", ierror.RaiseMsgCause("Unable to create directory "+path, err)
		}
	} else if errors.Is(err, fs.ErrExist) && !info.IsDir() {
		return "", ierror.RaiseMsg("Unable to create directory " + path)
	}

	strIndex := strconv.FormatInt(index, 10)
	zeros := utils.Max(6-len(strIndex), 0)
	return path + "/part" + strings.Repeat("0", zeros) + strIndex, nil
}

func (this *IIOImpl) openFileRead(path string) (*os.File, error) {
	logger.Info("IO: opening file ", path)
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	} else if !info.Mode().IsRegular() {
		return nil, ierror.RaiseMsg(path + " was not a file")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" cannot be opened", err)
	}
	logger.Info("IO: file opening successful")
	return file, nil
}

func (this *IIOImpl) openFileWrite(path string) (*os.File, error) {
	logger.Info("IO: creating file ", path)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrExist) {
		if o, err := this.executorData.GetProperties().IoOverwrite(); err != nil {
			return nil, ierror.Raise(err)
		} else {
			if o {
				logger.Warn("IO: ", path, " already exists")
				if err = os.Remove(path); err != nil {
					return nil, ierror.RaiseMsgCause(path+" can not be removed", err)
				}
			} else {
				return nil, ierror.RaiseMsg(path + " already exists")
			}
		}
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" cannot be opened", err)
	}
	logger.Info("IO: file created successful")
	return file, nil
}

func (this *IIOImpl) ioCores() (int, error) {
	cores, err := this.executorData.GetProperties().IoCores()
	if err != nil {
		return 1, ierror.Raise(err)
	}
	if cores > 1 {
		return utils.Min(this.executorData.GetCores(), int(math.Ceil(cores))), nil
	}
	return utils.Max(1, int(math.Ceil(cores*float64(this.executorData.GetCores())))), nil
}
