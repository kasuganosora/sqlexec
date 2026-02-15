package builtin

import (
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/utils"
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
		// === Batch 2: Extended Date/Time Functions ===
		{Name: "extract", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "extract", ReturnType: "integer", ParamTypes: []string{"string", "datetime"}, Variadic: false}}, Handler: dateExtract, Description: "提取日期部分（年/月/日/时/分/秒等）", Example: "EXTRACT('year', '2024-03-15') -> 2024", Category: "date"},
		{Name: "date_part", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "date_part", ReturnType: "integer", ParamTypes: []string{"string", "datetime"}, Variadic: false}}, Handler: dateExtract, Description: "提取日期部分（extract的别名）", Example: "DATE_PART('month', '2024-03-15') -> 3", Category: "date"},
		{Name: "date_add", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "date_add", ReturnType: "datetime", ParamTypes: []string{"datetime", "integer", "string"}, Variadic: false}}, Handler: dateAdd, Description: "日期加上间隔", Example: "DATE_ADD('2024-01-01', 1, 'month') -> '2024-02-01'", Category: "date"},
		{Name: "adddate", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "adddate", ReturnType: "datetime", ParamTypes: []string{"datetime", "integer", "string"}, Variadic: false}}, Handler: dateAdd, Description: "日期加上间隔（date_add的别名）", Example: "ADDDATE('2024-01-01', 1, 'month') -> '2024-02-01'", Category: "date"},
		{Name: "date_sub", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "date_sub", ReturnType: "datetime", ParamTypes: []string{"datetime", "integer", "string"}, Variadic: false}}, Handler: dateSub, Description: "日期减去间隔", Example: "DATE_SUB('2024-02-01', 1, 'month') -> '2024-01-01'", Category: "date"},
		{Name: "subdate", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "subdate", ReturnType: "datetime", ParamTypes: []string{"datetime", "integer", "string"}, Variadic: false}}, Handler: dateSub, Description: "日期减去间隔（date_sub的别名）", Example: "SUBDATE('2024-02-01', 1, 'month') -> '2024-01-01'", Category: "date"},
		{Name: "date_trunc", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "date_trunc", ReturnType: "datetime", ParamTypes: []string{"string", "datetime"}, Variadic: false}}, Handler: dateTrunc, Description: "截断日期到指定精度", Example: "DATE_TRUNC('month', '2024-03-15 12:30:00') -> '2024-03-01'", Category: "date"},
		{Name: "datetrunc", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "datetrunc", ReturnType: "datetime", ParamTypes: []string{"string", "datetime"}, Variadic: false}}, Handler: dateTrunc, Description: "截断日期到指定精度（date_trunc的别名）", Example: "DATETRUNC('month', '2024-03-15') -> '2024-03-01'", Category: "date"},
		{Name: "dayname", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "dayname", ReturnType: "string", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayName, Description: "返回星期名称", Example: "DAYNAME('2024-01-01') -> 'Monday'", Category: "date"},
		{Name: "day_name", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "day_name", ReturnType: "string", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayName, Description: "返回星期名称", Example: "DAY_NAME('2024-01-01') -> 'Monday'", Category: "date"},
		{Name: "monthname", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "monthname", ReturnType: "string", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateMonthName, Description: "返回月份名称", Example: "MONTHNAME('2024-03-15') -> 'March'", Category: "date"},
		{Name: "month_name", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "month_name", ReturnType: "string", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateMonthName, Description: "返回月份名称", Example: "MONTH_NAME('2024-03-15') -> 'March'", Category: "date"},
		{Name: "dayofweek", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "dayofweek", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayOfWeek, Description: "返回星期几（1=Sunday, 7=Saturday）", Example: "DAYOFWEEK('2024-01-01') -> 2", Category: "date"},
		{Name: "day_of_week", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "day_of_week", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayOfWeek, Description: "返回星期几（1=Sunday, 7=Saturday）", Example: "DAY_OF_WEEK('2024-01-01') -> 2", Category: "date"},
		{Name: "dayofyear", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "dayofyear", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayOfYear, Description: "返回一年中的第几天", Example: "DAYOFYEAR('2024-03-01') -> 61", Category: "date"},
		{Name: "day_of_year", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "day_of_year", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDayOfYear, Description: "返回一年中的第几天", Example: "DAY_OF_YEAR('2024-03-01') -> 61", Category: "date"},
		{Name: "week", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "week", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateWeek, Description: "返回ISO周数", Example: "WEEK('2024-03-15') -> 11", Category: "date"},
		{Name: "weekofyear", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "weekofyear", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateWeek, Description: "返回ISO周数（week的别名）", Example: "WEEKOFYEAR('2024-03-15') -> 11", Category: "date"},
		{Name: "quarter", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "quarter", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateQuarter, Description: "返回季度（1-4）", Example: "QUARTER('2024-03-15') -> 1", Category: "date"},
		{Name: "last_day", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "last_day", ReturnType: "date", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateLastDay, Description: "返回当月最后一天", Example: "LAST_DAY('2024-02-15') -> '2024-02-29'", Category: "date"},
		{Name: "unix_timestamp", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "unix_timestamp", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: true}}, Handler: dateUnixTimestamp, Description: "返回Unix时间戳", Example: "UNIX_TIMESTAMP('2024-01-01') -> 1704067200", Category: "date"},
		{Name: "epoch", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "epoch", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: true}}, Handler: dateUnixTimestamp, Description: "返回Unix时间戳（unix_timestamp的别名）", Example: "EPOCH('2024-01-01') -> 1704067200", Category: "date"},
		{Name: "from_unixtime", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "from_unixtime", ReturnType: "datetime", ParamTypes: []string{"integer"}, Variadic: false}}, Handler: dateFromUnixtime, Description: "从Unix时间戳转换为日期时间", Example: "FROM_UNIXTIME(1704067200) -> '2024-01-01 00:00:00'", Category: "date"},
		{Name: "to_timestamp", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "to_timestamp", ReturnType: "datetime", ParamTypes: []string{"integer"}, Variadic: false}}, Handler: dateFromUnixtime, Description: "从Unix时间戳转换为日期时间（from_unixtime的别名）", Example: "TO_TIMESTAMP(1704067200) -> '2024-01-01 00:00:00'", Category: "date"},
		{Name: "make_date", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "make_date", ReturnType: "date", ParamTypes: []string{"integer", "integer", "integer"}, Variadic: false}}, Handler: dateMakeDate, Description: "从年月日构造日期", Example: "MAKE_DATE(2024, 3, 15) -> '2024-03-15'", Category: "date"},
		{Name: "make_time", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "make_time", ReturnType: "time", ParamTypes: []string{"integer", "integer", "integer"}, Variadic: false}}, Handler: dateMakeTime, Description: "从时分秒构造时间", Example: "MAKE_TIME(12, 30, 45) -> '12:30:45'", Category: "date"},
		{Name: "make_timestamp", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "make_timestamp", ReturnType: "datetime", ParamTypes: []string{"integer", "integer", "integer", "integer", "integer", "integer"}, Variadic: false}}, Handler: dateMakeTimestamp, Description: "从年月日时分秒构造时间戳", Example: "MAKE_TIMESTAMP(2024, 3, 15, 12, 30, 45) -> '2024-03-15 12:30:45'", Category: "date"},
		// === Batch 9: Advanced Date Functions ===
		{Name: "age", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "age", ReturnType: "string", ParamTypes: []string{"datetime", "datetime"}, Variadic: false}}, Handler: dateAge, Description: "Difference between two dates as 'X years Y months Z days'", Example: "AGE('2026-03-15', '2024-01-01') -> '2 years 2 months 14 days'", Category: "date"},
		{Name: "time_bucket", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "time_bucket", ReturnType: "datetime", ParamTypes: []string{"integer", "datetime"}, Variadic: false}}, Handler: dateTimeBucket, Description: "Truncate timestamp to interval boundary (seconds)", Example: "TIME_BUCKET(3600, '2024-01-01 12:34:56') -> '2024-01-01 12:00:00'", Category: "date"},
		{Name: "century", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "century", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateCentury, Description: "Century of the date", Example: "CENTURY('2024-03-15') -> 21", Category: "date"},
		{Name: "decade", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "decade", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateDecade, Description: "Decade of the date (year/10)", Example: "DECADE('2024-03-15') -> 202", Category: "date"},
		{Name: "millennium", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "millennium", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateMillennium, Description: "Millennium of the date", Example: "MILLENNIUM('2024-03-15') -> 3", Category: "date"},
		{Name: "era", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "era", ReturnType: "string", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateEra, Description: "AD if year>0, BC otherwise", Example: "ERA('2024-03-15') -> 'AD'", Category: "date"},
		{Name: "isodow", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "isodow", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateIsoDow, Description: "ISO day of week (1=Monday..7=Sunday)", Example: "ISODOW('2024-01-01') -> 1", Category: "date"},
		{Name: "iso_day_of_week", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "iso_day_of_week", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateIsoDow, Description: "ISO day of week (1=Monday..7=Sunday)", Example: "ISO_DAY_OF_WEEK('2024-01-01') -> 1", Category: "date"},
		{Name: "isoyear", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "isoyear", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateIsoYear, Description: "ISO 8601 year", Example: "ISOYEAR('2024-12-30') -> 2025", Category: "date"},
		{Name: "iso_year", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "iso_year", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateIsoYear, Description: "ISO 8601 year", Example: "ISO_YEAR('2024-12-30') -> 2025", Category: "date"},
		{Name: "julian_day", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "julian_day", ReturnType: "float", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateJulianDay, Description: "Julian Day Number", Example: "JULIAN_DAY('2024-01-01') -> 2460310.5", Category: "date"},
		{Name: "year_week", Type: FunctionTypeScalar, Signatures: []FunctionSignature{{Name: "year_week", ReturnType: "integer", ParamTypes: []string{"datetime"}, Variadic: false}}, Handler: dateYearWeek, Description: "Year*100 + ISO week number", Example: "YEAR_WEEK('2024-03-15') -> 202411", Category: "date"},
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

// === Batch 2: Extended Date/Time Function Implementations ===

// dateExtract extracts a date part from a datetime value.
func dateExtract(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("extract() requires exactly 2 arguments (part, datetime)")
	}
	part := strings.ToLower(toString(args[0]))
	t, err := toTime(args[1])
	if err != nil {
		return nil, err
	}
	switch part {
	case "year":
		return int64(t.Year()), nil
	case "month":
		return int64(t.Month()), nil
	case "day":
		return int64(t.Day()), nil
	case "hour":
		return int64(t.Hour()), nil
	case "minute":
		return int64(t.Minute()), nil
	case "second":
		return int64(t.Second()), nil
	case "microsecond":
		return int64(t.Nanosecond() / 1000), nil
	case "millisecond":
		return int64(t.Nanosecond() / 1000000), nil
	case "dow", "dayofweek", "day_of_week":
		return int64(t.Weekday()), nil
	case "doy", "dayofyear", "day_of_year":
		return int64(t.YearDay()), nil
	case "week":
		_, w := t.ISOWeek()
		return int64(w), nil
	case "quarter":
		return int64((t.Month()-1)/3 + 1), nil
	case "epoch":
		return t.Unix(), nil
	default:
		return nil, fmt.Errorf("extract: unknown part '%s'", part)
	}
}

// dateAdd adds an interval to a datetime.
func dateAdd(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("date_add() requires 2 or 3 arguments (datetime, interval[, unit])")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	interval, err := utils.ToInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("date_add: interval must be integer: %w", err)
	}
	unit := "day"
	if len(args) == 3 {
		unit = strings.ToLower(toString(args[2]))
	}
	return addInterval(t, int(interval), unit)
}

// dateSub subtracts an interval from a datetime.
func dateSub(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("date_sub() requires 2 or 3 arguments (datetime, interval[, unit])")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	interval, err := utils.ToInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("date_sub: interval must be integer: %w", err)
	}
	unit := "day"
	if len(args) == 3 {
		unit = strings.ToLower(toString(args[2]))
	}
	return addInterval(t, -int(interval), unit)
}

func addInterval(t time.Time, n int, unit string) (time.Time, error) {
	switch unit {
	case "year", "years":
		return t.AddDate(n, 0, 0), nil
	case "month", "months":
		return t.AddDate(0, n, 0), nil
	case "day", "days":
		return t.AddDate(0, 0, n), nil
	case "hour", "hours":
		return t.Add(time.Duration(n) * time.Hour), nil
	case "minute", "minutes":
		return t.Add(time.Duration(n) * time.Minute), nil
	case "second", "seconds":
		return t.Add(time.Duration(n) * time.Second), nil
	case "week", "weeks":
		return t.AddDate(0, 0, n*7), nil
	default:
		return time.Time{}, fmt.Errorf("unknown interval unit: '%s'", unit)
	}
}

// dateTrunc truncates a datetime to the specified precision.
func dateTrunc(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("date_trunc() requires exactly 2 arguments (unit, datetime)")
	}
	unit := strings.ToLower(toString(args[0]))
	t, err := toTime(args[1])
	if err != nil {
		return nil, err
	}
	loc := t.Location()
	switch unit {
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, loc), nil
	case "quarter":
		qMonth := time.Month(((int(t.Month())-1)/3)*3 + 1)
		return time.Date(t.Year(), qMonth, 1, 0, 0, 0, 0, loc), nil
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc), nil
	case "week":
		// ISO week: Monday is the first day
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		d := t.AddDate(0, 0, -(wd - 1))
		return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, loc), nil
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc), nil
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc), nil
	case "minute":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, loc), nil
	case "second":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc), nil
	default:
		return nil, fmt.Errorf("date_trunc: unknown unit '%s'", unit)
	}
}

func dateDayName(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("dayname() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return t.Weekday().String(), nil
}

func dateMonthName(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("monthname() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return t.Month().String(), nil
}

func dateDayOfWeek(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("dayofweek() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	// MySQL convention: 1=Sunday, 2=Monday, ..., 7=Saturday
	return int64(t.Weekday()) + 1, nil
}

func dateDayOfYear(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("dayofyear() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return int64(t.YearDay()), nil
}

func dateWeek(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("week() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	_, w := t.ISOWeek()
	return int64(w), nil
}

func dateQuarter(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("quarter() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return int64((t.Month()-1)/3 + 1), nil
}

func dateLastDay(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("last_day() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	// First day of next month minus 1 day
	first := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	last := first.AddDate(0, 0, -1)
	return last, nil
}

func dateUnixTimestamp(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return time.Now().Unix(), nil
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("unix_timestamp() requires 0 or 1 argument")
	}
	if args[0] == nil {
		return time.Now().Unix(), nil
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return t.Unix(), nil
}

func dateFromUnixtime(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("from_unixtime() requires exactly 1 argument")
	}
	epoch, err := utils.ToInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("from_unixtime: argument must be integer: %w", err)
	}
	return time.Unix(epoch, 0).UTC(), nil
}

func dateMakeDate(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("make_date() requires exactly 3 arguments (year, month, day)")
	}
	y, err := utils.ToInt64(args[0])
	if err != nil {
		return nil, err
	}
	m, err := utils.ToInt64(args[1])
	if err != nil {
		return nil, err
	}
	d, err := utils.ToInt64(args[2])
	if err != nil {
		return nil, err
	}
	return time.Date(int(y), time.Month(m), int(d), 0, 0, 0, 0, time.UTC), nil
}

func dateMakeTime(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("make_time() requires exactly 3 arguments (hour, minute, second)")
	}
	h, err := utils.ToInt64(args[0])
	if err != nil {
		return nil, err
	}
	mi, err := utils.ToInt64(args[1])
	if err != nil {
		return nil, err
	}
	s, err := utils.ToInt64(args[2])
	if err != nil {
		return nil, err
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, mi, s), nil
}

func dateMakeTimestamp(args []interface{}) (interface{}, error) {
	if len(args) != 6 {
		return nil, fmt.Errorf("make_timestamp() requires exactly 6 arguments (year, month, day, hour, minute, second)")
	}
	vals := make([]int64, 6)
	for i := 0; i < 6; i++ {
		v, err := utils.ToInt64(args[i])
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	return time.Date(int(vals[0]), time.Month(vals[1]), int(vals[2]),
		int(vals[3]), int(vals[4]), int(vals[5]), 0, time.UTC), nil
}

// === Batch 9: Advanced Date Function Implementations ===

// dateAge returns the difference between two dates as "X years Y months Z days".
func dateAge(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("age() requires exactly 2 arguments")
	}
	a, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	b, err := toTime(args[1])
	if err != nil {
		return nil, err
	}

	// Ensure a >= b for positive results; swap if needed and track sign.
	negative := false
	if a.Before(b) {
		a, b = b, a
		negative = true
	}

	years := a.Year() - b.Year()
	months := int(a.Month()) - int(b.Month())
	days := a.Day() - b.Day()

	if days < 0 {
		months--
		// Days in the previous month of 'a'
		prev := time.Date(a.Year(), a.Month(), 0, 0, 0, 0, 0, time.UTC)
		days += prev.Day()
	}
	if months < 0 {
		years--
		months += 12
	}

	if negative {
		return fmt.Sprintf("-%d years %d months %d days", years, months, days), nil
	}
	return fmt.Sprintf("%d years %d months %d days", years, months, days), nil
}

// dateTimeBucket truncates a timestamp to an interval boundary.
// args[0] = interval in seconds, args[1] = datetime
func dateTimeBucket(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("time_bucket() requires exactly 2 arguments (interval_seconds, datetime)")
	}
	interval, err := utils.ToInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("time_bucket: interval must be integer: %w", err)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("time_bucket: interval must be positive")
	}
	t, err := toTime(args[1])
	if err != nil {
		return nil, err
	}
	unix := t.Unix()
	truncated := unix - (unix % interval)
	return time.Unix(truncated, 0).UTC(), nil
}

// dateCentury returns (year-1)/100 + 1.
func dateCentury(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("century() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	y := int64(t.Year())
	if y > 0 {
		return (y-1)/100 + 1, nil
	}
	// For BC years, century is negative
	return (y-1)/100 + 1, nil
}

// dateDecade returns year/10.
func dateDecade(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("decade() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	return int64(t.Year()) / 10, nil
}

// dateMillennium returns (year-1)/1000 + 1.
func dateMillennium(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("millennium() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	y := int64(t.Year())
	if y > 0 {
		return (y-1)/1000 + 1, nil
	}
	return (y-1)/1000 + 1, nil
}

// dateEra returns "AD" if year > 0, "BC" otherwise.
func dateEra(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("era() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	if t.Year() > 0 {
		return "AD", nil
	}
	return "BC", nil
}

// dateIsoDow returns the ISO day of week: 1=Monday..7=Sunday.
func dateIsoDow(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("isodow() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	wd := int64(t.Weekday())
	if wd == 0 {
		wd = 7 // Sunday = 7
	}
	return wd, nil
}

// dateIsoYear returns the ISO 8601 year.
func dateIsoYear(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("isoyear() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	y, _ := t.ISOWeek()
	return int64(y), nil
}

// dateJulianDay computes the Julian Day Number for a given date.
func dateJulianDay(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("julian_day() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}

	y := int64(t.Year())
	m := int64(t.Month())
	d := int64(t.Day())

	// Julian Day Number formula (for Gregorian calendar)
	// https://en.wikipedia.org/wiki/Julian_day#Converting_Gregorian_calendar_date_to_Julian_Day_Number
	a := (14 - m) / 12
	yAdj := y + 4800 - a
	mAdj := m + 12*a - 3
	jdn := d + (153*mAdj+2)/5 + 365*yAdj + yAdj/4 - yAdj/100 + yAdj/400 - 32045

	// Add fractional day for time component
	frac := (float64(t.Hour()) - 12.0) / 24.0
	frac += float64(t.Minute()) / 1440.0
	frac += float64(t.Second()) / 86400.0

	return float64(jdn) + frac, nil
}

// dateYearWeek returns year*100 + ISO week number.
func dateYearWeek(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("year_week() requires exactly 1 argument")
	}
	t, err := toTime(args[0])
	if err != nil {
		return nil, err
	}
	y, w := t.ISOWeek()
	return int64(y)*100 + int64(w), nil
}
