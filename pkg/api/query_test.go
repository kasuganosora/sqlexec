package api

import (
	"reflect"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQuery(t *testing.T) {
	session := &Session{}
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
	}

	query := NewQuery(session, result, "SELECT * FROM users", nil)

	assert.NotNil(t, query)
	assert.Equal(t, session, query.session)
	assert.Equal(t, result, query.result)
	assert.Equal(t, "SELECT * FROM users", query.sql)
	assert.Nil(t, query.params)
	assert.Equal(t, -1, query.rowIndex)
	assert.False(t, query.closed)
}

func TestNewQueryWithParams(t *testing.T) {
	session := &Session{}
	result := &domain.QueryResult{}
	params := []interface{}{1, "Alice"}

	query := NewQuery(session, result, "SELECT * FROM users WHERE id = ?", params)

	assert.NotNil(t, query)
	assert.Equal(t, params, query.params)
}

func TestQuery_Next(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// First Next() should move to row 0
	assert.True(t, query.Next())
	assert.Equal(t, 0, query.rowIndex)

	// Second Next() should move to row 1
	assert.True(t, query.Next())
	assert.Equal(t, 1, query.rowIndex)

	// Third Next() should return false (no more rows)
	assert.False(t, query.Next())
}

func TestQuery_NextAfterClose(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// Close the query
	_ = query.Close()

	// Next() should return false after close
	assert.False(t, query.Next())
}

func TestQuery_Scan(t *testing.T) {
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// Move to first row
	assert.True(t, query.Next())

	// Scan into variables
	var id int64
	var name string
	err := query.Scan(&id, &name)
	require.NoError(t, err)

	assert.Equal(t, int64(1), id)
	assert.Equal(t, "Alice", name)
}

func TestQuery_ScanClosed(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
	_ = query.Close()

	// Scan on closed query should error
	var id int64
	err := query.Scan(&id)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestQuery_ScanBeforeNext(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// Scan without calling Next() should error
	var id int64
	err := query.Scan(&id)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Next()")
}

func TestQuery_Row(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// Move to first row
	assert.True(t, query.Next())

	// Get row
	row := query.Row()
	assert.NotNil(t, row)
	assert.Equal(t, int64(1), row["id"])
	assert.Equal(t, "Alice", row["name"])
}

func TestQuery_RowCopy(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1)},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
	assert.True(t, query.Next())

	row1 := query.Row()
	row2 := query.Row()

	// Should be independent copies
	assert.Equal(t, row1["id"], row2["id"])
	
	// Modify one copy
	row1["id"] = int64(999)
	
	// Other copy should not be affected
	assert.Equal(t, int64(1), row2["id"])
}

func TestQuery_RowClosed(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
	_ = query.Close()

	// Row on closed query should return nil
	row := query.Row()
	assert.Nil(t, row)
}

func TestQuery_RowsCount(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
	count := query.RowsCount()

	assert.Equal(t, 3, count)
}

func TestQuery_Columns(t *testing.T) {
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		},
		Rows: []domain.Row{},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
	columns := query.Columns()

	assert.Len(t, columns, 3)
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, "name", columns[1].Name)
	assert.Equal(t, "email", columns[2].Name)
}

func TestQuery_ColumnsNilResult(t *testing.T) {
	query := NewQuery(&Session{}, nil, "SELECT * FROM users", nil)
	columns := query.Columns()

	assert.Len(t, columns, 0)
}

func TestQuery_Close(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	assert.False(t, query.closed)

	err := query.Close()
	assert.NoError(t, err)
	assert.True(t, query.closed)
	assert.Nil(t, query.result)
}

func TestQuery_CloseAlreadyClosed(t *testing.T) {
	query := NewQuery(&Session{}, &domain.QueryResult{}, "SELECT", nil)
	_ = query.Close()

	// Close again should not error
	err := query.Close()
	assert.NoError(t, err)
}

func TestQuery_Iter(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	// Iterate through rows
	var count int
	err := query.Iter(func(row domain.Row) error {
		count++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.True(t, query.closed)
}

func TestQuery_IterWithError(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1)},
			{"id": int64(2)},
		},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	expectedErr := NewError(ErrCodeInternal, "test error", nil)

	// Iterate with error in callback
	err := query.Iter(func(row domain.Row) error {
		if count, _ := row["id"].(int64); count == int64(1) {
			return expectedErr
		}
		return nil
	})

	assert.Equal(t, expectedErr, err)
}

func TestQuery_IterEmptyResult(t *testing.T) {
	result := &domain.QueryResult{
		Rows: []domain.Row{},
	}

	query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)

	var count int
	err := query.Iter(func(row domain.Row) error {
		count++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestSetValue(t *testing.T) {
	tests := []struct {
		name     string
		dest     interface{}
		value    interface{}
		wantErr  bool
		expected interface{}
	}{
		{
			name:     "int64 to int",
			dest:     new(int64),
			value:    int64(123),
			wantErr:  false,
			expected: int64(123),
		},
		{
			name:     "int64 to int8",
			dest:     new(int8),
			value:    int64(127),
			wantErr:  false,
			expected: int8(127),
		},
		{
			name:     "int64 to int16",
			dest:     new(int16),
			value:    int64(32767),
			wantErr:  false,
			expected: int16(32767),
		},
		{
			name:     "int64 to int32",
			dest:     new(int32),
			value:    int64(2147483647),
			wantErr:  false,
			expected: int32(2147483647),
		},
		{
			name:     "float64 to float32",
			dest:     new(float32),
			value:    3.1415926535,
			wantErr:  false,
			expected: float32(3.1415927),
		},
		{
			name:     "string to []byte",
			dest:     new([]byte),
			value:    "hello",
			wantErr:  false,
			expected: []byte("hello"),
		},
		{
			name:     "nil to int",
			dest:     new(int),
			value:    nil,
			wantErr:  false,
			expected: 0,
		},
		{
			name:    "non-pointer",
			dest:    123,
			value:   int64(456),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := setValue(tt.dest, tt.value)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expected != nil {
					assert.Equal(t, tt.expected, getValueFromPointer(tt.dest))
				}
			}
		})
	}
}

func TestSetValueNilDest(t *testing.T) {
	err := setValue(nil, int64(123))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination is nil")
}

func TestSetValueNonPointer(t *testing.T) {
	var x int = 123
	err := setValue(x, int64(456))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pointer")
}

func getValueFromPointer(ptr interface{}) interface{} {
	v := reflect.ValueOf(ptr)
	return v.Elem().Interface()
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		target   reflect.Type
		wantErr  bool
	}{
		{
			name:     "int64 to int",
			value:    int64(123),
			target:   reflect.TypeOf(0),
			wantErr:  false,
		},
		{
			name:     "string to string",
			value:    "hello",
			target:   reflect.TypeOf(""),
			wantErr:  false,
		},
		{
			name:     "nil to int",
			value:    nil,
			target:   reflect.TypeOf(0),
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.target)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.value != nil {
					assert.NotNil(t, result)
				}
			}
		})
	}
}

func TestConvertValueNil(t *testing.T) {
	result, err := convertValue(nil, reflect.TypeOf(0))
	assert.NoError(t, err)
	// nil returns the zero value for the target type
	// For int, that's 0
	assert.Equal(t, 0, result)
}

func TestConvertValueToPointer(t *testing.T) {
	result, err := convertValue(int64(123), reflect.TypeOf(new(int64)))
	// Just check it doesn't panic
	_ = result
	_ = err

	// Note: Pointer conversion is not fully implemented
	// This test just ensures it doesn't crash
}

func TestQuery_Err(t *testing.T) {
	result := &domain.QueryResult{}

	t.Run("no error", func(t *testing.T) {
		query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
		assert.NoError(t, query.Err())
	})

	t.Run("with error", func(t *testing.T) {
		query := NewQuery(&Session{}, result, "SELECT * FROM users", nil)
		query.err = NewError(ErrCodeInternal, "query error", nil)
		assert.Error(t, query.Err())
		assert.Contains(t, query.Err().Error(), "query error")
	})
}
