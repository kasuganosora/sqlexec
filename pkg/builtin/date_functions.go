package builtin

import (
	"fmt"
	"time"
)

func init() {
	// 注册日期时间函数
	dateFunctions := []*FunctionInfo{
		{
			Name: "now",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "now", ReturnType: "datetime", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateNow,
			Description: "返回当前日期时间",
			Example:     "NOW() -> '2024-01-01 12:00:00'",
			Category:    "date",
		},
		{
			Name: "current_timestamp",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "current_timestamp", ReturnType: "datetime", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateNow,
			Description: "返回当前时间戳",
			Example:     "CURRENT_TIMESTAMP() -> '2024-01-01 12:00:00'",
			Category:    "date",
		},
		{
			Name: "current_date",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "current_date", ReturnType: "date", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateCurrentDate,
			Description: "返回当前日期",
			Example:     "CURRENT_DATE() -> '2024-01-01'",
			Category:    "date",
		},
		{
			Name: "curdate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "curdate", ReturnType: "date", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateCurrentDate,
			Description: "返回当前日期",
			Example:     "CURDATE() -> '2024-01-01'",
			Category:    "date",
		},
		{
			Name: "current_time",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "current_time", ReturnType: "time", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateCurrentTime,
			Description: "返回当前时间",
			Example:     "CURRENT_TIME() -> '12:00:00'",
			Category:    "date",
		},
		{
			Name: "curtime",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "curtime", ReturnType: "time", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     dateCurrentTime,
			Description: "返回当前时间",
			Example:     "CURTIME() -> '12:00:00'",
			Category:    "date",
		},
		{
			Name: "year",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "year", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateYear,
			Description: "返回年份",
			Example:     "YEAR('2024-01-01') -> 2024",
			Category:    "date",
		},
		{
			Name: "month",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "month", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateMonth,
			Description: "返回月份",
			Example:     "MONTH('2024-01-01') -> 1",
			Category:    "date",
		},
		{
			Name: "day",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "day", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateDay,
			Description: "返回日期",
			Example:     "DAY('2024-01-01') -> 1",
			Category:    "date",
		},
		{
			Name: "dayofmonth",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "dayofmonth", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateDay,
			Description: "返回日期（day的别名）",
			Example:     "DAYOFMONTH('2024-01-01') -> 1",
			Category:    "date",
		},
		{
			Name: "hour",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "hour", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateHour,
			Description: "返回小时",
			Example:     "HOUR('2024-01-01 12:30:00') -> 12",
			Category:    "date",
		},
		{
			Name: "minute",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "minute", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateMinute,
			Description: "返回分钟",
			Example:     "MINUTE('2024-01-01 12:30:00') -> 30",
			Category:    "date",
		},
		{
			Name: "second",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "second", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateSecond,
			Description: "返回秒",
			Example:     "SECOND('2024-01-01 12:30:45') -> 45",
			Category:    "date",
		},
		{
			Name: "date",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "date", ReturnType: "date", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateDate,
			Description: "提取日期部分",
			Example:     "DATE('2024-01-01 12:30:00') -> '2024-01-01'",
			Category:    "date",
		},
		{
			Name: "time",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "time", ReturnType: "time", ParamTypes: []string{"datetime"}, Variadic: false},
			},
			Handler:     dateTime,
			Description: "提取时间部分",
			Example:     "TIME('2024-01-01 12:30:00') -> '12:30:00'",
			Category:    "date",
		},
		{
			Name: "date_format",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "date_format", ReturnType: "string", ParamTypes: []string{"datetime", "string"}, Variadic: false},
			},
			Handler:     dateFormat,
			Description: "格式化日期",
			Example:     "DATE_FORMAT('2024-01-01', '%Y-%m-%d') -> '2024-01-01'",
			Category:    "date",
		},
		{
			Name: "datediff",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "datediff", ReturnType: "integer", ParamTypes: []string{"datetime", "datetime"}, Variadic: false},
			},
			Handler:     dateDiff,
			Description: "日期差（天）",
			Example:     "DATEDIFF('2024-01-02', '2024-01-01') -> 1",
			Category:    "date",
		},
	}

	for _, fn := range dateFunctions {
		RegisterGlobal(fn)
	}
}

// 辅助函数：将参数转换为time.Time
func toTime(arg interface{}) (time.Time, error) {
	switch v := arg.(type) {
	case time.Time:
		return v, nil
	case string:
		// 尝试解析常见的日期格式
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", arg)
	}
}

// 日期时间函数实现
func dateNow(args []interface{}) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("now() requires no arguments")
	}
	return time.Now(), nil
}

func dateCurrentDate(args []interface{}) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("current_date() requires no arguments")
	}
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()), nil
}

func dateCurrentTime(args []interface{}) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("current_time() requires no arguments")
	}
	now := time.Now()
	return now.Format("15:04:05"), nil
}

func dateYear(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("year() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Year()), nil
}

func dateMonth(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("month() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Month()), nil
}

func dateDay(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("day() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Day()), nil
}

func dateHour(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("hour() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Hour()), nil
}

func dateMinute(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("minute() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Minute()), nil
}

func dateSecond(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("second() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return int64(t.Second()), nil
}

func dateDate(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("date() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}

func dateTime(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("time() requires exactly 1 argument")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	return t.Format("15:04:05"), nil
}

func dateFormat(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("date_format() requires exactly 2 arguments")
	}
	
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	format := toString(args[1])
	
	// 简化实现，支持常见格式
	layout := "2006-01-02 15:04:05"
	if format == "%Y-%m-%d" {
		layout = "2006-01-02"
	} else if format == "%H:%i:%s" {
		layout = "15:04:05"
	} else if format == "%Y-%m-%d %H:%i:%s" {
		layout = "2006-01-02 15:04:05"
	}
	
	return t.Format(layout), nil
}

func dateDiff(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("datediff() requires exactly 2 arguments")
	}
	
	t1, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	
	t2, err := toTime(args[1])
	if err != nil {
		return nil, err
	}
	
	diff := t1.Sub(t2)
	return int64(diff.Hours() / 24), nil
}
