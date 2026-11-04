package src

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// ApplyLimits caps CPU cores and sets a soft memory limit; 0 leaves a limit unset.
func ApplyLimits(cfg Config) {
	if cfg.MaxCPUs > 0 {
		runtime.GOMAXPROCS(cfg.MaxCPUs)
	}
	if cfg.MemoryLimitMB > 0 {
		debug.SetMemoryLimit(int64(cfg.MemoryLimitMB) << 20)
	}
	logMessage(fmt.Sprintf("Limits: CPU=%d, memory=%d MB (0 = unlimited).", cfg.MaxCPUs, cfg.MemoryLimitMB), "trace")
}
