package product

type ProductConfig struct {
	MaxBatchCount     int
	MaxIoWorkerCount  int64
	LingerMs          int64
	ExtMap            map[string]string
	IsInjectionEngine bool //是否注入引擎
}

func GetDefaultProductConfig() *ProductConfig {
	return &ProductConfig{
		MaxBatchCount:    10,
		MaxIoWorkerCount: 64,
		LingerMs:         20,
	}
}
