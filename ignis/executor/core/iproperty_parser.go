package core

type IPropertyParser struct {
}

func NewIPropertyParser() *IPropertyParser {
	return &IPropertyParser{
	}
}

func (this *IPropertyParser) Cores() (int64, error) { return this.GetNumber("ignis.executor.cores") }

func (this *IPropertyParser) TransportCores() (float64, error) {
	return this.GetMinDouble("ignis.transport.cores", 0)
}

func (this *IPropertyParser) PartitionMinimal() (int64, error) {
	return this.GetSize("ignis.partition.minimal")
}

func (this *IPropertyParser) SortSamples() (float64, error) {
	return this.GetMinDouble("ignis.modules.sort.samples", 0)
}

func (this *IPropertyParser) SortResampling() (bool, error) {
	return this.GetBool("ignis.modules.sort.resampling")
}

func (this *IPropertyParser) IoOverwrite() (bool, error) {
	return this.GetBool("ignis.modules.io.overwrite")
}

func (this *IPropertyParser) IoCores() (float64, error) {
	return this.GetMinDouble("ignis.modules.io.cores", 0)
}

func (this *IPropertyParser) IoCompression() (int64, error) {
	return this.GetRangeNumber("ignis.modules.io.compression", 0, 9)
}

func (this *IPropertyParser) MsgCompression() (int64, error) {
	return this.GetRangeNumber("ignis.transport.compression", 0, 9)
}

func (this *IPropertyParser) PartitionCompression() (int64, error) {
	return this.GetRangeNumber("ignis.partition.compression", 0, 9)
}

func (this *IPropertyParser) TransportElemSize() (int64, error) {
	return this.GetSize("ignis.transport.element.size")
}

func (this *IPropertyParser) PartitionType() (string, error) {
	return this.GetString("ignis.partition.type")
}

func (this *IPropertyParser) JobDirectory() (string, error) {
	return this.GetString("ignis.job.directory")
}

func (this *IPropertyParser) ExecutorDirectory() (string, error) {
	return this.GetString("ignis.executor.directory")
}

func (this *IPropertyParser) GetString(key string) (string, error) {
	return "", nil
}

func (this *IPropertyParser) GetNumber(key string) (int64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetFloat(key string) (float64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetMinNumber(key string, min int64) (int64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetMaxNumber(key string, max int64) (int64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetRangeNumber(key string, min int64, max int64) (int64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetMinDouble(key string, min float64) (float64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetMaxDouble(key string, max float64) (float64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetRangeDouble(key string, min float64, max float64) (float64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetSize(key string) (int64, error) {
	return 0, nil
}

func (this *IPropertyParser) GetBool(key string) (bool, error) {
	return false, nil
}
