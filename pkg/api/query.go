package api

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Query 查询结果对象
type Query struct {
	session  *Session
	result   *domain.QueryResult
	sql      string
	params   []interface{}    // 查询参数（用于缓存键）
	rowIndex int
	closed   bool
	mu       sync.RWMutex
	err      error
}

// NewQuery 创建 Query
func NewQuery(session *Session, result *domain.QueryResult, sql string, params []interface{}) *Query {
	return &Query{
		session:  session,
		result:   result,
		sql:      sql,
		params:   params,
		rowIndex: -1,
		closed:   false,
	}
}

// Err returns the error that occurred during query execution
func (q *Query) Err() error {
	return q.err
}

// Next 移动到下一行
func (q *Query) Next() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.result == nil {
		return false
	}

	q.rowIndex++
	return q.rowIndex < len(q.result.Rows)
}

// Scan 扫描当前行到变量
func (q *Query) Scan(dest ...interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.result == nil {
		return NewError(ErrCodeClosed, "Query is closed", nil)
	}

	if q.rowIndex < 0 {
		return NewError(ErrCodeInvalidParam, "Next() must be called before Scan()", nil)
	}

	if q.rowIndex >= len(q.result.Rows) {
		return fmt.Errorf("no more rows")
	}

	row := q.result.Rows[q.rowIndex]

	if len(dest) > len(q.result.Columns) {
		return fmt.Errorf("too many destination variables (%d), have %d columns",
			len(dest), len(q.result.Columns))
	}

	// 按列顺序扫描
	for i, colInfo := range q.result.Columns {
		if i >= len(dest) {
			break
		}

		value, exists := row[colInfo.Name]
		if !exists {
			continue
		}

		// 反射设置值
		if err := setValue(dest[i], value); err != nil {
			return fmt.Errorf("failed to scan column %s: %w", colInfo.Name, err)
		}
	}

	return nil
}

// Row 获取当前行（map 形式）
func (q *Query) Row() domain.Row {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed || q.result == nil || q.rowIndex < 0 || q.rowIndex >= len(q.result.Rows) {
		return nil
	}

	// 返回行的深拷贝
	row := q.result.Rows[q.rowIndex]
	list := make(domain.Row, len(row))
	for k, v := range row {
		list[k] = v
	}

	return list
}

// RowsCount 获取总行数
func (q *Query) RowsCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.result == nil {
		return 0
	}
	return len(q.result.Rows)
}

// Columns 获取列信息
func (q *Query) Columns() []domain.ColumnInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.result == nil {
		return []domain.ColumnInfo{}
	}

	cols := make([]domain.ColumnInfo, len(q.result.Columns))
	copy(cols, q.result.Columns)
	return cols
}

// Close 关闭查询
func (q *Query) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	q.closed = true
	q.result = nil
	return nil
}

// Iter 遍历所有行（回调函数）
func (q *Query) Iter(fn func(row domain.Row) error) error {
	defer q.Close()

	for q.Next() {
		row := q.Row()
		if err := fn(row); err != nil {
			return err
		}
	}

	return nil
}

// setValue 设置值到目标变量
func setValue(dest interface{}, value interface{}) error {
	if dest == nil {
		return fmt.Errorf("destination is nil")
	}

	// 使用反射设置值
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destValue = destValue.Elem()
	if !destValue.CanSet() {
		return fmt.Errorf("destination cannot be set")
	}

	// 如果值是 nil，设置零值
	if value == nil {
		destValue.Set(reflect.Zero(destValue.Type()))
		return nil
	}

	valValue := reflect.ValueOf(value)

	// 类型转换
	destType := destValue.Type()
	valType := valValue.Type()

	// 如果类型相同，直接赋值
	if valType == destType {
		destValue.Set(valValue)
		return nil
	}

	// 尝试类型转换
	converted, err := convertValue(value, destType)
	if err != nil {
		return err
	}

	destValue.Set(reflect.ValueOf(converted))
	return nil
}

// convertValue 转换值类型
func convertValue(value interface{}, targetType reflect.Type) (interface{}, error) {
	if value == nil {
		return reflect.Zero(targetType).Interface(), nil
	}

	valueType := reflect.TypeOf(value)

	// 如果类型相同，直接返回
	if valueType == targetType {
		return value, nil
	}

	// 目标类型是指针，解引用并返回指针
	if targetType.Kind() == reflect.Ptr {
		converted, err := convertValue(value, targetType.Elem())
		if err != nil {
			return nil, err
		}
		// 创建一个新的指针值
		ptr := reflect.New(targetType.Elem())
		ptr.Elem().Set(reflect.ValueOf(converted))
		return ptr.Interface(), nil
	}

	// 处理 int64 到 int 的转换
	if targetType.Kind() == reflect.Int && valueType.Kind() == reflect.Int64 {
		return int(value.(int64)), nil
	}

	// 处理 int64 到 int8/int16/int32
	switch targetType.Kind() {
	case reflect.Int8:
		return int8(value.(int64)), nil
	case reflect.Int16:
		return int16(value.(int64)), nil
	case reflect.Int32:
		return int32(value.(int64)), nil
	}

	// 处理 float64 到 float32
	if targetType.Kind() == reflect.Float32 && valueType.Kind() == reflect.Float64 {
		return float32(value.(float64)), nil
	}

	// 处理 string 到 []byte
	if targetType.Kind() == reflect.Slice && targetType.Elem().Kind() == reflect.Uint8 {
		if str, ok := value.(string); ok {
			return []byte(str), nil
		}
	}

	// 默认：直接返回
	return value, nil
}
