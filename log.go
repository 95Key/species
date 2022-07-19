package species

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logsPath = "./logs"

func NewLog() *zap.SugaredLogger {
	_, err := os.Stat(logsPath)
	if os.IsNotExist(err) {
		if err := os.Mkdir(logsPath, os.ModePerm); err != nil {
			panic(err)
		}
	}
	encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	file, err := os.OpenFile(fmt.Sprintf("./logs/%s.log", time.Now().Format(TIME_LAYOUT_YYMMDD)), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	writeSync := zapcore.AddSync(file)
	core := zapcore.NewCore(encoder, writeSync, zap.DebugLevel)
	return zap.New(core, zap.AddCaller()).Sugar()
}
