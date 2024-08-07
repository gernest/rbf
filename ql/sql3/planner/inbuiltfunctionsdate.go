package planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/gernest/rbf/ql/core"
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
)

const intervalYear = "YY"
const intervalYearDay = "YD"
const intervalMonth = "M"
const intervalDay = "D"
const intervalWeeKDay = "W"
const intervalWeek = "WK"
const intervalHour = "HH"
const intervalMinute = "MI"
const intervalSecond = "S"
const intervalMillisecond = "MS"
const intervalMicrosecond = "US"
const intervalNanosecond = "NS"

func (p *ExecutionPlanner) analyzeFunctionDateTimePart(call *parser.Call, scope parser.Statement) (parser.Expr, error) {

	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// interval
	intervalType := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(intervalType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), intervalType.TypeDescription())
	}

	// date
	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), dateType.TypeDescription())
	}

	// return int
	call.ResultDataType = parser.NewDataTypeInt()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionToTimestamp(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	// param1 is the number to be converted to timestamp. This param is required.
	// param2 is the time unit of the numeric value in param 1. This param is optional.
	// ToTimestamp can be invoked with just param1.
	if len(call.Args) != 1 && len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	// param1 is a integer of type int64
	param1Type := parser.NewDataTypeInt()
	if !typesAreAssignmentCompatible(param1Type, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), param1Type.TypeDescription())
	}

	// param2 is a string and it should be one of 's', 'ms', 'us', 'ns'.
	// param2 is optional, will be defaulted to 's' if not supplied.
	if len(call.Args) == 2 {
		param2Type := parser.NewDataTypeString()
		if !typesAreAssignmentCompatible(param2Type, call.Args[1].DataType()) {
			return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), param2Type.TypeDescription())
		}
	}
	// ToTimestamp returns a timestamp calculated from param1 using time unit passed in param 2
	call.ResultDataType = parser.NewDataTypeTimestamp()
	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionDatetimeAdd(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	// param1 is the time unit of duration to be added to the target timestamp.
	// param2 is the time duration to be added to the target timestamp.
	// param3 is the target timestamp to which the time duration to be added.
	if len(call.Args) != 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 3, len(call.Args))
	}

	// param1- time unit is a string and it should be one of 'yy','m','d','hh','mi','s', 'ms', 'us', 'ns'.
	param1Type := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(param1Type, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), param1Type.TypeDescription())
	}

	// param2- time duration is a int
	param2Type := parser.NewDataTypeInt()
	if !typesAreAssignmentCompatible(param2Type, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), param2Type.TypeDescription())
	}

	// param3- target datetime to which the duration to be added to
	param3Type := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(param3Type, call.Args[2].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[2].Pos().Line, call.Args[2].Pos().Column, call.Args[2].DataType().TypeDescription(), param3Type.TypeDescription())
	}

	// DatetimeAdd returns a timestamp calculated by adding param2 to param3 using time unit passed in param 1
	call.ResultDataType = parser.NewDataTypeTimestamp()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionDateTimeFromParts(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 7 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 7, len(call.Args))
	}

	intType := parser.NewDataTypeInt()
	for _, part := range call.Args {
		if !typesAreAssignmentCompatible(intType, part.DataType()) {
			return nil, sql3.NewErrParameterTypeMistmatch(part.Pos().Line, part.Pos().Column, part.DataType().TypeDescription(), intType.TypeDescription())
		}
	}

	call.ResultDataType = parser.NewDataTypeTimestamp()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionDateTimeName(call *parser.Call, scope parser.Statement) (parser.Expr, error) {

	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// interval
	intervalType := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(intervalType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), intervalType.TypeDescription())
	}

	// date
	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), dateType.TypeDescription())
	}

	//return int
	call.ResultDataType = parser.NewDataTypeString()
	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionDateTrunc(call *parser.Call, scope parser.Statement) (parser.Expr, error) {

	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// interval
	intervalType := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(intervalType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), intervalType.TypeDescription())
	}

	// date
	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), dateType.TypeDescription())
	}

	//return int
	call.ResultDataType = parser.NewDataTypeString()
	return call, nil
}

// analyzeFunctionDateTimeDiff ensures a timeunit and start and end timestamps.
func (p *ExecutionPlanner) analyzeFunctionDateTimeDiff(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 3, len(call.Args))
	}

	// interval
	intervalType := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(intervalType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), intervalType.TypeDescription())
	}

	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), dateType.TypeDescription())
	}

	if !typesAreAssignmentCompatible(dateType, call.Args[2].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[2].Pos().Line, call.Args[2].Pos().Column, call.Args[2].DataType().TypeDescription(), dateType.TypeDescription())
	}

	call.ResultDataType = parser.NewDataTypeInt()
	return call, nil
}

func (n *callPlanExpression) EvaluateDateTimePart(currentRow []interface{}) (interface{}, error) {
	intervalEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	dateEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// nil if anything is nil
	if intervalEval == nil || dateEval == nil {
		return nil, nil
	}

	// get the date value
	coercedDate, err := coerceValue(n.args[1].Type(), parser.NewDataTypeTimestamp(), dateEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	date, dateOk := coercedDate.(time.Time)
	if !dateOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	// get the interval value
	coercedInterval, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), intervalEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	interval, intervalOk := coercedInterval.(string)
	if !intervalOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	switch strings.ToUpper(interval) {
	case intervalYear:
		return int64(date.Year()), nil

	case intervalYearDay:
		return int64(date.YearDay()), nil

	case intervalMonth:
		return int64(date.Month()), nil

	case intervalDay:
		return int64(date.Day()), nil

	case intervalWeeKDay:
		return int64(date.Weekday()), nil

	case intervalWeek:
		_, isoWeek := date.ISOWeek()
		return int64(isoWeek), nil

	case intervalHour:
		return int64(date.Hour()), nil

	case intervalMinute:
		return int64(date.Minute()), nil

	case intervalSecond:
		return int64(date.Second()), nil

	case intervalMillisecond:
		return int64(date.Nanosecond() / 1000000), nil

	case intervalMicrosecond:
		return int64(date.Nanosecond() / 1000), nil

	case intervalNanosecond:
		return int64(date.Nanosecond()), nil

	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, interval, "interval")
	}

}

// EvaluateDateTimeFromParts evaluates the call to date_time_from_parts. This uses the base time.Date() function.
func (n *callPlanExpression) EvaluateDateTimeFromParts(currentRow []interface{}) (interface{}, error) {
	timestamps := make([]int, len(n.args))
	for i, arg := range n.args {
		param, err := arg.Evaluate(currentRow)
		if err != nil {
			return nil, err
		} else if param == nil {
			return nil, nil
		}
		coercedValue, err := coerceValue(arg.Type(), parser.NewDataTypeInt(), param, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}
		val, ok := coercedValue.(int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert value")
		}
		timestamps[i] = int(val)
	}

	if val, ok := isValidDateTimeParts(timestamps); !ok {
		return nil, sql3.NewErrInvalidDatetimePart(0, 0, val)
	}

	dt := time.Date(timestamps[0], time.Month(timestamps[1]), timestamps[2], timestamps[3], timestamps[4], timestamps[5], timestamps[6]*1000*1000, time.UTC)
	if dt.Year() < 0 || dt.Year() > 9999 {
		return nil, sql3.NewErrInvalidDatetimePart(0, 0, dt.Year())
	}
	return dt, nil
}

// isValidDateTimeParts returns true if the year is between 0 and 9999 and the date and time exists(think of leap year).
// the argument is a slice of ints representing the year, month, day, hour, minutes, seconds, and milliseconds.
// If any value in the slice falls outside its range, the value and false are returned.
func isValidDateTimeParts(timestamps []int) (value int, ok bool) {
	if timestamps[0] < 0 || timestamps[0] > 9999 {
		return timestamps[0], false
	}
	if timestamps[1] < 1 || timestamps[1] > 12 {
		return timestamps[1], false
	}
	switch timestamps[1] {
	case 1, 3, 5, 7, 8, 10, 12:
		if timestamps[2] < 1 || timestamps[2] > 31 {
			return timestamps[2], false
		}
	case 4, 6, 9, 11:
		if timestamps[2] < 1 || timestamps[2] > 30 {
			return timestamps[2], false
		}
	case 2:
		if timestamps[2] < 1 || timestamps[2] > 29 {
			return timestamps[2], false
		}
		if !(timestamps[0]%4 == 0 && timestamps[0]%100 != 0 || timestamps[0]%400 == 0) {
			if timestamps[2] == 29 {
				return timestamps[2], false
			}
		}
	}
	if timestamps[3] < 0 || timestamps[3] > 23 {
		return timestamps[3], false
	}
	if timestamps[4] < 0 || timestamps[4] > 59 {
		return timestamps[4], false
	}
	if timestamps[5] < 0 || timestamps[5] > 59 {
		return timestamps[5], false
	}
	if timestamps[6] < 0 || timestamps[6] > 999 {
		return timestamps[6], false
	}
	return 0, true
}

func (n *callPlanExpression) EvaluateToTimestamp(currentRow []interface{}) (interface{}, error) {
	// retrieve param1, the number to be converted to timestamp
	param1, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	} else if param1 == nil {
		// if the param1 is null silently return null timestamp value
		return nil, nil
	}
	coercedParam1, err := coerceValue(n.args[0].Type(), parser.NewDataTypeInt(), param1, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		// raise error if param 1 is not an integer. Should we return nil instead of raising error here? see note at return.
		return nil, err
	}
	num, ok := coercedParam1.(int64)
	if !ok {
		// raise error if param 1 is not an integer. Should we return nil instead of raising error here? see note at return.
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	// retrieve param2, time unit for param1, if not supplied default to seconds 's'.
	var unit string = core.TimeUnitSeconds
	if len(n.args) == 2 {
		param2, err := n.args[1].Evaluate(currentRow)
		if err != nil {
			// raise error if unable to retieve the argument for param2
			return nil, err
		}
		coercedParam2, err := coerceValue(n.args[1].Type(), parser.NewDataTypeString(), param2, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			// raise error if param2 is not a string
			return nil, err
		}
		unit, ok = coercedParam2.(string)
		if !ok {
			// raise error if param2 is not a string
			return nil, sql3.NewErrInternalf("unable to convert value")
		}
		if !core.IsValidTimeUnit(unit) {
			// raise error is param2 is not a valid time unit
			return nil, sql3.NewErrCallParameterValueInvalid(0, 0, unit, "timeunit")
		}
	}
	// should we throw error or return nil if the conversion fails? what is the desired behaviour when ToTimestamp errors for one bad record in a batch of thousands?
	return core.ValToTimestamp(unit, num)
}

func (n *callPlanExpression) EvaluateDateTimeName(currentRow []interface{}) (interface{}, error) {
	intervalEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	dateEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// nil if anything is nil
	if intervalEval == nil || dateEval == nil {
		return nil, nil
	}

	//get the date value
	coercedDate, err := coerceValue(n.args[1].Type(), parser.NewDataTypeTimestamp(), dateEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	date, dateOk := coercedDate.(time.Time)
	if !dateOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	//get the interval value
	coercedInterval, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), intervalEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	interval, intervalOk := coercedInterval.(string)
	if !intervalOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	switch strings.ToUpper(interval) {
	case intervalYear:
		return fmt.Sprint(date.Year()), nil

	case intervalYearDay:
		return fmt.Sprint(date.YearDay()), nil

	case intervalMonth:
		return fmt.Sprint(date.Month()), nil

	case intervalDay:
		return fmt.Sprint(date.Day()), nil

	case intervalWeeKDay:
		return fmt.Sprint(date.Weekday()), nil

	case intervalWeek:
		_, isoWeek := date.ISOWeek()
		return fmt.Sprint(isoWeek), nil

	case intervalHour:
		return fmt.Sprint(date.Hour()), nil

	case intervalMinute:
		return fmt.Sprint(date.Minute()), nil

	case intervalSecond:
		return fmt.Sprint(date.Second()), nil

	case intervalMillisecond:
		return fmt.Sprint(date.Nanosecond() / 1000000), nil
	case intervalMicrosecond:
		return fmt.Sprint(date.Nanosecond() / 1000), nil
	case intervalNanosecond:
		return fmt.Sprint(date.Nanosecond()), nil

	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, interval, "interval")
	}
}

func (n *callPlanExpression) EvaluateDatetimeAdd(currentRow []interface{}) (interface{}, error) {
	// retrieve param1, timeunit of the value to be added to the target timestamp
	param1, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	coercedParam1, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), param1, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		// raise error if param 1 is not string.
		return nil, err
	}
	timeunit, ok := coercedParam1.(string)
	if !ok {
		// raise error if param 1 is not string.
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	// retrieve param2, timeduration to be added to the target timestamp.
	param2, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		// raise error if unable to retieve the argument for param2
		return nil, err
	}
	// retrieve param3, target timestamp to which the timeduration to be added to.
	param3, err := n.args[2].Evaluate(currentRow)
	if err != nil {
		// raise error if unable to retieve the argument for param3
		return nil, err
	}
	if param2 == nil || param3 == nil {
		// if either of timeduration or target datetime is null then return null
		return nil, nil
	}
	coercedParam2, err := coerceValue(n.args[1].Type(), parser.NewDataTypeInt(), param2, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		// raise error if param2 is not a string
		return nil, err
	}
	timeduration, ok := coercedParam2.(int64)
	if !ok {
		// raise error if param2 is not a integer
		return nil, sql3.NewErrInternalf("unable to convert value")
	}
	coercedParam3, err := coerceValue(n.args[2].Type(), parser.NewDataTypeTimestamp(), param3, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		// raise error if param3 is not a timestamp
		return nil, err
	}
	target, ok := coercedParam3.(time.Time)
	if !ok {
		// raise error if param3 is not a datetime
		return nil, sql3.NewErrInternalf("unable to convert value")
	}
	if !isValidTimeInterval(strings.ToUpper(timeunit)) {
		// raise error if timeunit value is invalid
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, timeunit, "timeunit")
	} else if target.IsZero() {
		// return nil if target is nil
		return nil, nil
	} else if timeduration == 0 {
		// return target if duration to add is 0
		return target, nil
	}
	switch strings.ToUpper(timeunit) {
	case intervalYear:
		return target.AddDate(int(timeduration), 0, 0), nil
	case intervalMonth:
		return target.AddDate(0, int(timeduration), 0), nil
	case intervalDay:
		return target.AddDate(0, 0, int(timeduration)), nil
	case intervalHour:
		return target.Add(time.Hour * time.Duration(timeduration)), nil
	case intervalMinute:
		return target.Add(time.Minute * time.Duration(timeduration)), nil
	case intervalSecond:
		return target.Add(time.Second * time.Duration(timeduration)), nil
	case intervalMillisecond:
		return target.Add(time.Millisecond * time.Duration(timeduration)), nil
	case intervalMicrosecond:
		return target.Add(time.Microsecond * time.Duration(timeduration)), nil
	case intervalNanosecond:
		return target.Add(time.Nanosecond * time.Duration(timeduration)), nil
	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, timeunit, "timeunit")
	}
}
func (n *callPlanExpression) EvaluateDateTrunc(currentRow []interface{}) (interface{}, error) {
	intervalEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	dateEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// nil if anything is nil
	if intervalEval == nil || dateEval == nil {
		return nil, nil
	}

	//get the date value
	coercedDate, err := coerceValue(n.args[1].Type(), parser.NewDataTypeTimestamp(), dateEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	date, dateOk := coercedDate.(time.Time)
	if !dateOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	//get the interval value
	coercedInterval, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), intervalEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	interval, intervalOk := coercedInterval.(string)
	if !intervalOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	switch strings.ToUpper(interval) {
	case intervalYear:
		return date.Format("2006"), nil
	case intervalMonth:
		return date.Format("2006-01"), nil
	case intervalDay:
		return date.Format("2006-01-02"), nil
	case intervalHour:
		return date.Format("2006-01-02T15"), nil
	case intervalMinute:
		return date.Format("2006-01-02T15:04"), nil
	case intervalSecond:
		return date.Format("2006-01-02T15:04:05"), nil
	case intervalMillisecond:
		return date.Format("2006-01-02T15:04:05.000"), nil
	case intervalMicrosecond:
		return date.Format("2006-01-02T15:04:05.000000"), nil
	case intervalNanosecond:
		return date.Format("2006-01-02T15:04:05.000000000"), nil
	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, interval, "interval")
	}
}

// isValidTimeInterval returns true if part is valid.
func isValidTimeInterval(unit string) bool {
	switch unit {
	case intervalYear, intervalYearDay, intervalMonth, intervalDay, intervalWeeKDay,
		intervalWeek, intervalHour, intervalMinute, intervalSecond, intervalMillisecond,
		intervalMicrosecond, intervalNanosecond:
		return true
	default:
		return false
	}
}

// EvaluateDatetimeDiff takes three arguments:
// 1. param1, timeunit of the value to be subtracted from the target timestamp
// 2. param2, starttime to be subtracted from the endtime timestamp
// 3. param3, endtime timestamp to which the starttime to be subtracted from.
// It returns the difference between the two timestamps.
func (n *callPlanExpression) EvaluateDatetimeDiff(currentRow []interface{}) (interface{}, error) {
	param1, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if param1 == nil {
		return nil, nil
	}
	cp, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), param1, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}
	timeunit, ok := cp.(string)
	if !ok {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	param2, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	coercedParam, err := coerceValue(n.args[1].Type(), parser.NewDataTypeTimestamp(), param2, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}
	if coercedParam == nil {
		return nil, nil
	}
	startDate, ok := coercedParam.(time.Time)
	if !ok {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}
	if coercedParam == nil {
		return nil, nil
	}
	param3, err := n.args[2].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if param3 == nil {
		return nil, nil
	}
	coercedParam, err = coerceValue(n.args[2].Type(), parser.NewDataTypeTimestamp(), param3, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}
	endDate, ok := coercedParam.(time.Time)
	if !ok {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}
	var diff int64
	switch strings.ToUpper(timeunit) {
	case intervalYear:
		diff = int64(endDate.Year() - startDate.Year())
	case intervalMonth:
		diff = int64((endDate.Year()-startDate.Year())*12 + int(endDate.Month()-startDate.Month()))
	case intervalDay:
		diff = int64(endDate.Sub(startDate).Hours() / 24)
	case intervalHour:
		diff = int64(endDate.Sub(startDate).Hours())
	case intervalMinute:
		diff = int64(endDate.Sub(startDate).Minutes())
	case intervalSecond:
		diff = int64(endDate.Sub(startDate).Seconds())
	case intervalMillisecond:
		diff = endDate.Sub(startDate).Milliseconds()
	case intervalMicrosecond:
		diff = endDate.Sub(startDate).Microseconds()
	case intervalNanosecond:
		diff = endDate.Sub(startDate).Nanoseconds()
	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, timeunit, "timeunit")
	}
	if diff == (-1<<63) || diff == ((1<<63)-1) {
		return nil, sql3.NewErrOutputValueOutOfRange(0, 0)
	}
	return diff, nil
}
