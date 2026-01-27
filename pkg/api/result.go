package api

import "fmt"

// Result 命令执行结果
type Result struct {
	RowsAffected int64
	LastInsertID int64
	err          error
}

// NewResult 创建 Result
func NewResult(rowsAffected, lastInsertID int64, err error) *Result {
	return &Result{
		RowsAffected: rowsAffected,
		LastInsertID: lastInsertID,
		err:          err,
	}
}

// LastError 获取错误
func (r *Result) Err() error {
	return r.err
}

// Error 实现 error 接口
func (r *Result) Error() string {
	if r.err == nil {
		return fmt.Sprintf("Result: RowsAffected=%d, LastInsertID=%d",
			r.RowsAffected, r.LastInsertID)
	}
	return r.err.Error()
}
