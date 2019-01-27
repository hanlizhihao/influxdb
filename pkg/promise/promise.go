package promise

import (
	"errors"
	"go.uber.org/zap"
)

func ExecuteNeedRetryAction(f func() error, then func(), errFunc func(err error), log *zap.Logger) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("!!! ExecuteNeedRetryAction occur error, ", zap.Any("error msg: ", err))
			errFunc(errors.New("Execute func occur error "))
		}
	}()
	var err error
	times := 0
Retry:
	err = f()
	if err != nil {
		log.Error("execute method failed, error message: ", zap.Error(err))
		if times < 3 {
			times++
			goto Retry
		}
		errFunc(err)
		return
	}
	then()
	return
}
