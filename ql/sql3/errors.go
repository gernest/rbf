package sql3

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/gernest/rbf/ql/sql3/parser"
)

type Code string

const (
	ErrInternal         Code = "ErrInternal"
	ErrUnsupported      Code = "ErrUnsupported"
	ErrCacheKeyNotFound Code = "ErrCacheKeyNotFound"

	// syntax/semantic errors
	ErrDuplicateColumn       Code = "ErrDuplicateColumn"
	ErrUnknownType           Code = "ErrUnknownType"
	ErrUnknownIdentifier     Code = "ErrUnknownIdentifier"
	ErrTopLimitCannotCoexist Code = "ErrTopLimitCannotCoexist"

	// type related errors
	ErrTypeIncompatibleWithBitwiseOperator               Code = "ErrTypeIncompatibleWithBitwiseOperator"
	ErrTypeIncompatibleWithLogicalOperator               Code = "ErrTypeIncompatibleWithLogicalOperator"
	ErrTypeIncompatibleWithEqualityOperator              Code = "ErrTypeIncompatibleWithEqualityOperator"
	ErrTypeIncompatibleWithComparisonOperator            Code = "ErrTypeIncompatibleWithComparisonOperator"
	ErrTypeIncompatibleWithArithmeticOperator            Code = "ErrTypeIncompatibleWithArithmeticOperator"
	ErrTypeIncompatibleWithConcatOperator                Code = "ErrTypeIncompatibleWithConcatOperator"
	ErrTypeIncompatibleWithLikeOperator                  Code = "ErrTypeIncompatibleWithLikeOperator"
	ErrTypeIncompatibleWithBetweenOperator               Code = "ErrTypeIncompatibleWithBetweenOperator"
	ErrTypeCannotBeUsedAsRangeSubscript                  Code = "ErrTypeCannotBeUsedAsRangeSubscript"
	ErrTypesAreNotEquatable                              Code = "ErrTypesAreNotEquatable"
	ErrTypeMismatch                                      Code = "ErrTypeMismatch"
	ErrIncompatibleTypesForRangeSubscripts               Code = "ErrIncompatibleTypesForRangeSubscripts"
	ErrExpressionListExpected                            Code = "ErrExpressionListExpected"
	ErrBooleanExpressionExpected                         Code = "ErrBooleanExpressionExpected"
	ErrIntExpressionExpected                             Code = "ErrIntExpressionExpected"
	ErrIntOrDecimalExpressionExpected                    Code = "ErrIntOrDecimalExpressionExpected"
	ErrIntOrDecimalOrTimestampExpressionExpected         Code = "ErrIntOrDecimalOrTimestampExpressionExpected"
	ErrIntOrDecimalOrTimestampOrStringExpressionExpected Code = "ErrIntOrDecimalOrTimestampOrStringExpressionExpected"
	ErrStringExpressionExpected                          Code = "ErrStringExpressionExpected"
	ErrSetExpressionExpected                             Code = "ErrSetExpressionExpected"
	ErrTimeQuantumExpressionExpected                     Code = "ErrTimeQuantumExpressionExpected"
	ErrSingleRowExpected                                 Code = "ErrSingleRowExpected"

	// decimal
	ErrDecimalScaleExpected Code = "ErrDecimalScaleExpected"

	ErrInvalidCast         Code = "ErrInvalidCast"
	ErrInvalidTypeCoercion Code = "ErrInvalidTypeCoercion"

	ErrLiteralExpected                  Code = "ErrLiteralExpected"
	ErrIntegerLiteral                   Code = "ErrIntegerLiteral"
	ErrStringLiteral                    Code = "ErrStringLiteral"
	ErrBoolLiteral                      Code = "ErrBoolLiteral"
	ErrLiteralNullNotAllowed            Code = "ErrLiteralNullNotAllowed"
	ErrLiteralEmptySetNotAllowed        Code = "ErrLiteralEmptySetNotAllowed"
	ErrLiteralEmptyTupleNotAllowed      Code = "ErrLiteralEmptyTupleNotAllowed"
	ErrSetLiteralMustContainIntOrString Code = "ErrSetLiteralMustContainIntOrString"
	ErrInvalidColumnInFilterExpression  Code = "ErrInvalidColumnInFilterExpression"
	ErrInvalidTypeInFilterExpression    Code = "ErrInvalidTypeInFilterExpression"

	ErrTypeAssignmentIncompatible              Code = "ErrTypeAssignmentIncompatible"
	ErrTypeAssignmentToTimeQuantumIncompatible Code = "ErrTypeAssignmentToTimeQuantumIncompatible"

	ErrInvalidUngroupedColumnReference         Code = "ErrInvalidUngroupedColumnReference"
	ErrInvalidUngroupedColumnReferenceInHaving Code = "ErrInvalidUngroupedColumnReferenceInHaving"

	ErrInvalidTimeUnit    Code = "ErrInvalidTimeUnit"
	ErrInvalidTimeEpoch   Code = "ErrInvalidTimeEpoch"
	ErrInvalidTimeQuantum Code = "ErrInvalidTimeQuantum"
	ErrInvalidDuration    Code = "ErrInvalidDuration"

	ErrInsertExprTargetCountMismatch   Code = "ErrInsertExprTargetCountMismatch"
	ErrInsertMustHaveIDColumn          Code = "ErrInsertMustHaveIDColumn"
	ErrInsertMustAtLeastOneNonIDColumn Code = "ErrInsertMustAtLeastOneNonIDColumn"

	ErrDatabaseNotFound      Code = "ErrDatabaseNotFound"
	ErrDatabaseExists        Code = "ErrDatabaseExists"
	ErrInvalidDatabaseOption Code = "ErrInvalidDatabaseOption"
	ErrInvalidUnitsValue     Code = "ErrInvalidUnitsValue"

	ErrTableMustHaveIDColumn     Code = "ErrTableMustHaveIDColumn"
	ErrTableIDColumnType         Code = "ErrTableIDColumnType"
	ErrTableIDColumnConstraints  Code = "ErrTableIDColumnConstraints"
	ErrTableIDColumnAlter        Code = "ErrTableIDColumnAlter"
	ErrTableNotFound             Code = "ErrTableNotFound"
	ErrTableExists               Code = "ErrTableExists"
	ErrColumnNotFound            Code = "ErrColumnNotFound"
	ErrTableColumnNotFound       Code = "ErrTableColumnNotFound"
	ErrInvalidKeyPartitionsValue Code = "ErrInvalidKeyPartitionsValue"

	ErrTableOrViewNotFound Code = "ErrTableOrViewNotFound"

	ErrViewExists   Code = "ErrViewExists"
	ErrViewNotFound Code = "ErrViewNotFound"

	ErrModelExists   Code = "ErrModelExists"
	ErrModelNotFound Code = "ErrModelNotFound"

	ErrBadColumnConstraint         Code = "ErrBadColumnConstraint"
	ErrConflictingColumnConstraint Code = "ErrConflictingColumnConstraint"

	// expected errors
	ErrExpectedColumnReference         Code = "ErrExpectedColumnReference"
	ErrExpectedSortExpressionReference Code = "ErrExpectedSortExpressionReference"
	ErrExpectedSortableExpression      Code = "ErrExpectedSortableExpression"

	// call errors
	ErrCallUnknownFunction                  Code = "ErrCallUnknownFunction"
	ErrCallParameterCountMismatch           Code = "ErrCallParameterCountMismatch"
	ErrIdColumnNotValidForAggregateFunction Code = "ErrIdColumnNotValidForAggregateFunction"
	ErrParameterTypeMistmatch               Code = "ErrParameterTypeMistmatch"
	ErrCallParameterValueInvalid            Code = "ErrCallParameterValueInvalid"

	// insert errors

	ErrInsertValueOutOfRange            Code = "ErrInsertValueOutOfRange"
	ErrUnexpectedTimeQuantumTupleLength Code = "ErrUnexpectedTimeQuantumTupleLength"

	// bulk insert errors

	ErrReadingDatasource       Code = "ErrReadingDatasource"
	ErrMappingFromDatasource   Code = "ErrMappingFromDatasource"
	ErrFormatSpecifierExpected Code = "ErrFormatSpecifierExpected"
	ErrInvalidFormatSpecifier  Code = "ErrInvalidFormatSpecifier"
	ErrInputSpecifierExpected  Code = "ErrInputSpecifierExpected"
	ErrInvalidInputSpecifier   Code = "ErrInvalidInputSpecifier"
	ErrInvalidBatchSize        Code = "ErrInvalidBatchSize"
	ErrTypeConversionOnMap     Code = "ErrTypeConversionOnMap"
	ErrParsingJSON             Code = "ErrParsingJSON"
	ErrEvaluatingJSONPathExpr  Code = "ErrEvaluatingJSONPathExpr"

	// optimizer errors
	ErrAggregateNotAllowedInGroupBy Code = "ErrIdPercentileNotAllowedInGroupBy"

	// function evaluation
	ErrValueOutOfRange          Code = "ErrValueOutOfRange"
	ErrStringLengthMismatch     Code = "ErrStringLengthMismatch"
	ErrUnexpectedTypeConversion Code = "ErrUnexpectedTypeConversion"

	// time quantum function eval
	ErrQRangeFromAndToTimeCannotBeBothNull Code = "ErrQRangeFromAndToTimeCannotBeBothNull"
	ErrQRangeInvalidUse                    Code = "ErrQRangeInvalidUse"
	ErrInvalidDatetimePart                 Code = "ErrInvalidDatetimePart"
	ErrOutputValueOutOfRange               Code = "ErrOutputValueOutOfRange"
	ErrDivideByZero                        Code = "ErrDivideByZero"

	// remote execution
	ErrRemoteUnauthorized Code = "ErrRemoteUnauthorized"

	// query hints
	ErrUnknownQueryHint               Code = "ErrInvalidQueryHint"
	ErrInvalidQueryHintParameterCount Code = "ErrInvalidQueryHintParameterCount"

	// show options
	ErrUnknownShowOption Code = "ErrUnknownShowOption"
)

const (
	ErrOrganizationIDDoesNotExist Code = "OrganizationIDDoesNotExist"

	ErrDatabaseIDExists         Code = "DatabaseIDExists"
	ErrDatabaseIDDoesNotExist   Code = "DatabaseIDDoesNotExist"
	ErrDatabaseNameDoesNotExist Code = "DatabaseNameDoesNotExist"
	ErrDatabaseNameExists       Code = "DatabaseNameExists"

	ErrTableIDExists         Code = "TableIDExists"
	ErrTableKeyExists        Code = "TableKeyExists"
	ErrTableNameExists       Code = "TableNameExists"
	ErrTableIDDoesNotExist   Code = "TableIDDoesNotExist"
	ErrTableKeyDoesNotExist  Code = "TableKeyDoesNotExist"
	ErrTableNameDoesNotExist Code = "TableNameDoesNotExist"

	ErrFieldExists       Code = "FieldExists"
	ErrFieldDoesNotExist Code = "FieldDoesNotExist"

	ErrInvalidTransaction Code = "InvalidTransaction"

	ErrUnimplemented Code = "Unimplemented"
)

type codedError struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Wrapped string `json:"wrapped,omitempty"`
}

func (ce codedError) Error() string {
	if ce.Wrapped != "" {
		return ce.Wrapped
	}
	return ce.Message
}

func (ce codedError) Is(err error) bool {
	if e, ok := err.(codedError); ok && ce.Code == e.Code {
		return true
	}
	return false
}

// Is is a fork of the Is() method from `pkg/errors` which takes as its target
// an error Code instead of an error.
func Is(err error, target Code) bool {
	match := codedError{
		Code: target,
	}
	return errors.Is(err, match)
}

func NewErrDuplicateColumn(line int, col int, column string) error {
	return newError(
		ErrDuplicateColumn,
		fmt.Sprintf("[%d:%d] duplicate column '%s'", line, col, column),
	)
}

func newError(code Code, msg string) *codedError {
	return &codedError{Code: code, Message: msg}
}

func NewErrUnknownType(line int, col int, typ string) error {
	return newError(
		ErrUnknownType,
		fmt.Sprintf("[%d:%d] unknown type '%s'", line, col, typ),
	)
}

func NewErrUnknownIdentifier(line int, col int, ident string) error {
	return newError(
		ErrUnknownIdentifier,
		fmt.Sprintf("[%d:%d] unknown identifier '%s'", line, col, ident),
	)
}

func NewErrErrTopLimitCannotCoexist(line int, col int) error {
	return newError(
		ErrTopLimitCannotCoexist,
		fmt.Sprintf("[%d:%d] TOP and LIMIT cannot cannot be used at the same time (TOP will be deprecated in a future release)", line, col),
	)
}

func NewErrInternal(msg string) error {
	preamble := "internal error"
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		preamble = fmt.Sprintf("internal error (%s:%d)", filename, line)
	}
	errorMessage := fmt.Sprintf("%s %s", preamble, msg)
	return newError(
		ErrInternal,
		errorMessage,
	)
}

func NewErrInternalf(format string, a ...interface{}) error {
	preamble := "internal error"
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		preamble = fmt.Sprintf("internal error (%s:%d)", filename, line)
	}
	errorMessage := fmt.Sprintf(format, a...)
	errorMessage = fmt.Sprintf("%s %s", preamble, errorMessage)
	return newError(
		ErrInternal,
		errorMessage,
	)
}

func NewErrUnsupported(line, col int, is bool, thing string) error {
	msg := fmt.Sprintf("[%d:%d] %s are not supported", line, col, thing)
	if is {
		msg = fmt.Sprintf("[%d:%d] %s is not supported", line, col, thing)
	}
	return newError(
		ErrUnknownIdentifier,
		msg,
	)
}

func NewErrCacheKeyNotFound(key uint64) error {
	return newError(
		ErrCacheKeyNotFound,
		fmt.Sprintf("key '%d' not found", key),
	)
}

func NewErrTypeAssignmentIncompatible(line, col int, type1, type2 string) error {
	return newError(
		ErrTypeAssignmentIncompatible,
		fmt.Sprintf("[%d:%d] an expression of type '%s' cannot be assigned to type '%s'", line, col, type1, type2),
	)
}

func NewErrInvalidUngroupedColumnReference(line, col int, column string) error {
	return newError(
		ErrInvalidUngroupedColumnReference,
		fmt.Sprintf("[%d:%d] column '%s' invalid in select list because it is not aggregated or grouped", line, col, column),
	)
}

func NewErrInvalidUngroupedColumnReferenceInHaving(line, col int, column string) error {
	return newError(
		ErrInvalidUngroupedColumnReferenceInHaving,
		fmt.Sprintf("[%d:%d] column '%s' invalid in the having clause because it is not contained in an aggregate or the GROUP BY clause", line, col, column),
	)
}

func NewErrInvalidCast(line, col int, from, to string) error {
	return newError(
		ErrInvalidCast,
		fmt.Sprintf("[%d:%d] '%s' cannot be cast to '%s'", line, col, from, to),
	)
}

func NewErrInvalidTypeCoercion(line, col int, from, to string) error {
	return newError(
		ErrInvalidTypeCoercion,
		fmt.Sprintf("[%d:%d] unable to convert '%s' to type '%s'", line, col, from, to),
	)
}

func NewErrTypeAssignmentToTimeQuantumIncompatible(line, col int, type1 string) error {
	return newError(
		ErrTypeAssignmentToTimeQuantumIncompatible,
		fmt.Sprintf("[%d:%d] an expression of type '%s' cannot be assigned to a timequantum", line, col, type1),
	)
}

func NewErrLiteralExpected(line, col int) error {
	return newError(
		ErrLiteralExpected,
		fmt.Sprintf("[%d:%d] literal expression expected", line, col),
	)
}

func NewErrIntegerLiteral(line, col int) error {
	return newError(
		ErrIntegerLiteral,
		fmt.Sprintf("[%d:%d] integer literal expected", line, col),
	)
}

func NewErrStringLiteral(line, col int) error {
	return newError(
		ErrStringLiteral,
		fmt.Sprintf("[%d:%d] string literal expected", line, col),
	)
}

func NewErrBoolLiteral(line, col int) error {
	return newError(
		ErrBoolLiteral,
		fmt.Sprintf("[%d:%d] bool literal expected", line, col),
	)
}

func NewErrLiteralEmptySetNotAllowed(line, col int) error {
	return newError(
		ErrLiteralEmptySetNotAllowed,
		fmt.Sprintf("[%d:%d] set literal must contain at least one member", line, col),
	)
}

func NewErrSetLiteralMustContainIntOrString(line, col int) error {
	return newError(
		ErrSetLiteralMustContainIntOrString,
		fmt.Sprintf("[%d:%d] set literal must contain ints or strings", line, col),
	)
}

func NewErrLiteralNullNotAllowed(line, col int) error {
	return newError(
		ErrLiteralNullNotAllowed,
		fmt.Sprintf("[%d:%d] null literal not allowed", line, col),
	)
}

func NewErrInvalidColumnInFilterExpression(line, col int, column string, op string) error {
	return newError(
		ErrInvalidColumnInFilterExpression,
		fmt.Sprintf("[%d:%d] '%s' column cannot be used in a %s filter expression", line, col, column, op),
	)
}

func NewErrInvalidTypeInFilterExpression(line, col int, typeName string, op string) error {
	return newError(
		ErrInvalidTypeInFilterExpression,
		fmt.Sprintf("[%d:%d] unsupported type '%s' for %s filter expression", line, col, typeName, op),
	)
}

func NewErrLiteralEmptyTupleNotAllowed(line, col int) error {
	return newError(
		ErrLiteralEmptyTupleNotAllowed,
		fmt.Sprintf("[%d:%d] tuple literal must contain at least one member", line, col),
	)
}

func NewErrTypeIncompatibleWithBitwiseOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithBitwiseOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithLogicalOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithLogicalOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithEqualityOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithEqualityOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithComparisonOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithComparisonOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithArithmeticOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithArithmeticOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithConcatOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithConcatOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithLikeOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithLikeOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithBetweenOperator(line, col int, operator, type1 string) error {
	return newError(
		ErrTypeIncompatibleWithBetweenOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeCannotBeUsedAsRangeSubscript(line, col int, type1 string) error {
	return newError(
		ErrTypeCannotBeUsedAsRangeSubscript,
		fmt.Sprintf("[%d:%d] type '%s' cannot be used as a range subscript", line, col, type1),
	)
}

func NewErrIncompatibleTypesForRangeSubscripts(line, col int, type1 string, type2 string) error {
	return newError(
		ErrIncompatibleTypesForRangeSubscripts,
		fmt.Sprintf("[%d:%d] incompatible types '%s' and '%s' used as range subscripts", line, col, type1, type2),
	)
}

func NewErrTypesAreNotEquatable(line, col int, type1, type2 string) error {
	return newError(
		ErrTypesAreNotEquatable,
		fmt.Sprintf("[%d:%d] types '%s' and '%s' are not equatable", line, col, type1, type2),
	)
}

func NewErrTypeMismatch(line, col int, type1, type2 string) error {
	return newError(
		ErrTypeMismatch,
		fmt.Sprintf("[%d:%d] types '%s' and '%s' do not match", line, col, type1, type2),
	)
}

func NewErrExpressionListExpected(line, col int) error {
	return newError(
		ErrExpressionListExpected,
		fmt.Sprintf("[%d:%d] expression list expected", line, col),
	)
}

func NewErrBooleanExpressionExpected(line, col int) error {
	return newError(
		ErrBooleanExpressionExpected,
		fmt.Sprintf("[%d:%d] boolean expression expected", line, col),
	)
}

func NewErrIntExpressionExpected(line, col int) error {
	return newError(
		ErrIntExpressionExpected,
		fmt.Sprintf("[%d:%d] integer expression expected", line, col),
	)
}

func NewErrIntOrDecimalExpressionExpected(line, col int) error {
	return newError(
		ErrIntOrDecimalExpressionExpected,
		fmt.Sprintf("[%d:%d] integer or decimal expression expected", line, col),
	)
}

func NewErrIntOrDecimalOrTimestampExpressionExpected(line, col int) error {
	return newError(
		ErrIntOrDecimalOrTimestampExpressionExpected,
		fmt.Sprintf("[%d:%d] integer, decimal or timestamp expression expected", line, col),
	)
}

func NewErrIntOrDecimalOrTimestampOrStringExpressionExpected(line, col int) error {
	return newError(
		ErrIntOrDecimalOrTimestampOrStringExpressionExpected,
		fmt.Sprintf("[%d:%d] integer, decimal, timestamp or string expression expected", line, col),
	)
}

func NewErrStringExpressionExpected(line, col int) error {
	return newError(
		ErrStringExpressionExpected,
		fmt.Sprintf("[%d:%d] string expression expected", line, col),
	)
}

func NewErrSetExpressionExpected(line, col int) error {
	return newError(
		ErrSetExpressionExpected,
		fmt.Sprintf("[%d:%d] set expression expected", line, col),
	)
}

func NewErrTimeQuantumExpressionExpected(line, col int) error {
	return newError(
		ErrTimeQuantumExpressionExpected,
		fmt.Sprintf("[%d:%d] time quantum expression expected", line, col),
	)
}

func NewErrSingleRowExpected(line, col int) error {
	return newError(
		ErrSingleRowExpected,
		fmt.Sprintf("[%d:%d] single row expected", line, col),
	)
}

// type errors

// decimal related

func NewErrDecimalScaleExpected(line, col int) error {
	return newError(
		ErrDecimalScaleExpected,
		fmt.Sprintf("[%d:%d] decimal scale expected", line, col),
	)
}

func NewErrInvalidTimeUnit(line, col int, unit string) error {
	return newError(
		ErrInvalidTimeUnit,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time unit", line, col, unit),
	)
}

func NewErrInvalidTimeEpoch(line, col int, epoch string) error {
	return newError(
		ErrInvalidTimeEpoch,
		fmt.Sprintf("[%d:%d] '%s' is not a valid epoch", line, col, epoch),
	)
}

func NewErrInvalidTimeQuantum(line, col int, quantum string) error {
	return newError(
		ErrInvalidTimeQuantum,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time quantum", line, col, quantum),
	)
}

func NewErrInvalidDuration(line, col int, duration string) error {
	return newError(
		ErrInvalidDuration,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time duration", line, col, duration),
	)
}

func NewErrInsertExprTargetCountMismatch(line int, col int) error {
	return newError(
		ErrInsertExprTargetCountMismatch,
		fmt.Sprintf("[%d:%d] mismatch in the count of expressions and target columns", line, col),
	)
}

func NewErrInsertMustHaveIDColumn(line int, col int) error {
	return newError(
		ErrInsertMustHaveIDColumn,
		fmt.Sprintf("[%d:%d] insert column list must have '_id' column specified", line, col),
	)
}

func NewErrInsertMustAtLeastOneNonIDColumn(line int, col int) error {
	return newError(
		ErrInsertMustAtLeastOneNonIDColumn,
		fmt.Sprintf("[%d:%d] insert column list must have at least one non '_id' column specified", line, col),
	)
}

func NewErrDatabaseExists(line, col int, databaseName string) error {
	return newError(
		ErrDatabaseExists,
		fmt.Sprintf("[%d:%d] database '%s' already exists", line, col, databaseName),
	)
}

func NewErrTableMustHaveIDColumn(line, col int) error {
	return newError(
		ErrTableMustHaveIDColumn,
		fmt.Sprintf("[%d:%d] _id column must be specified", line, col),
	)
}

func NewErrTableIDColumnType(line, col int) error {
	return newError(
		ErrTableIDColumnType,
		fmt.Sprintf("[%d:%d] _id column must be specified with type ID or STRING", line, col),
	)
}

func NewErrTableIDColumnConstraints(line, col int) error {
	return newError(
		ErrTableIDColumnConstraints,
		fmt.Sprintf("[%d:%d] _id column must be specified with no constraints", line, col),
	)
}

func NewErrTableIDColumnAlter(line, col int) error {
	return newError(
		ErrTableIDColumnAlter,
		fmt.Sprintf("[%d:%d] _id column cannot be added to an existing table", line, col),
	)
}

func NewErrDatabaseNotFound(line, col int, databaseName string) error {
	return newError(
		ErrDatabaseNotFound,
		fmt.Sprintf("[%d:%d] database '%s' not found", line, col, databaseName),
	)
}

func NewErrTableNotFound(line, col int, tableName string) error {
	return newError(
		ErrTableNotFound,
		fmt.Sprintf("[%d:%d] table '%s' not found", line, col, tableName),
	)
}

func NewErrTableOrViewNotFound(line, col int, tableName string) error {
	return newError(
		ErrTableOrViewNotFound,
		fmt.Sprintf("[%d:%d] table or view '%s' not found", line, col, tableName),
	)
}

func NewErrTableExists(line, col int, tableName string) error {
	return newError(
		ErrTableExists,
		fmt.Sprintf("[%d:%d] table or view '%s' already exists", line, col, tableName),
	)
}

func NewErrColumnNotFound(line, col int, columnName string) error {
	return newError(
		ErrColumnNotFound,
		fmt.Sprintf("[%d:%d] column '%s' not found", line, col, columnName),
	)
}

func NewErrTableColumnNotFound(line, col int, tableName string, columnName string) error {
	return newError(
		ErrTableColumnNotFound,
		fmt.Sprintf("[%d:%d] column '%s' not found in table '%s'", line, col, columnName, tableName),
	)
}

func NewErrInvalidDatabaseOption(line, col int, option string) error {
	return newError(
		ErrInvalidDatabaseOption,
		fmt.Sprintf("[%d:%d] invalid database option '%s'", line, col, option),
	)
}

func NewErrInvalidUnitsValue(line, col int, units int64) error {
	return newError(
		ErrInvalidUnitsValue,
		fmt.Sprintf("[%d:%d] invalid value '%d' for units (should be a number between 0-10000)", line, col, units),
	)
}

func NewErrInvalidKeyPartitionsValue(line, col int, keypartitions int64) error {
	return newError(
		ErrInvalidKeyPartitionsValue,
		fmt.Sprintf("[%d:%d] invalid value '%d' for key partitions (should be a number between 1-10000)", line, col, keypartitions),
	)
}

func NewErrViewNotFound(line, col int, viewName string) error {
	return newError(
		ErrViewNotFound,
		fmt.Sprintf("[%d:%d] view '%s' not found", line, col, viewName),
	)
}

func NewErrViewExists(line, col int, viewName string) error {
	return newError(
		ErrViewExists,
		fmt.Sprintf("[%d:%d] view '%s' already exists", line, col, viewName),
	)
}

func NewErrModelNotFound(line, col int, viewName string) error {
	return newError(
		ErrModelNotFound,
		fmt.Sprintf("[%d:%d] model '%s' not found", line, col, viewName),
	)
}

func NewErrModelExists(line, col int, viewName string) error {
	return newError(
		ErrModelExists,
		fmt.Sprintf("[%d:%d] model '%s' already exists", line, col, viewName),
	)
}

func NewErrBadColumnConstraint(line, col int, constraint, columnType string) error {
	return newError(
		ErrBadColumnConstraint,
		fmt.Sprintf("[%d:%d] '%s' constraint cannot be applied to a column of type '%s'", line, col, constraint, columnType),
	)
}

func NewErrConflictingColumnConstraint(line, col int, token1, token2 parser.Token) error {
	return newError(
		ErrConflictingColumnConstraint,
		fmt.Sprintf("[%d:%d] '%s' constraint conflicts with '%s'", line, col, token1, token2),
	)
}

// expected

func NewErrExpectedColumnReference(line, col int) error {
	return newError(
		ErrExpectedColumnReference,
		fmt.Sprintf("[%d:%d] column reference expected", line, col),
	)
}

func NewErrExpectedSortExpressionReference(line, col int) error {
	return newError(
		ErrExpectedSortExpressionReference,
		fmt.Sprintf("[%d:%d] column reference, alias reference or column position expected", line, col),
	)
}

func NewErrExpectedSortableExpression(line, col int, typeName string) error {
	return newError(
		ErrExpectedSortExpressionReference,
		fmt.Sprintf("[%d:%d] unable to sort a column of type '%s'", line, col, typeName),
	)
}

// calls

func NewErrCallParameterCountMismatch(line, col int, functionName string, formalCount, actualCount int) error {
	return newError(
		ErrCallParameterCountMismatch,
		fmt.Sprintf("[%d:%d] '%s': count of formal parameters (%d) does not match count of actual parameters (%d)", line, col, functionName, formalCount, actualCount),
	)
}

func NewErrCallUnknownFunction(line, col int, functionName string) error {
	return newError(
		ErrCallUnknownFunction,
		fmt.Sprintf("[%d:%d] unknown function '%s'", line, col, functionName),
	)
}

func NewErrIdColumnNotValidForAggregateFunction(line, col int, functionName string) error {
	return newError(
		ErrIdColumnNotValidForAggregateFunction,
		fmt.Sprintf("[%d:%d] _id column cannot be used in aggregate function '%s'", line, col, functionName),
	)
}

func NewErrParameterTypeMistmatch(line, col int, type1, type2 string) error {
	return newError(
		ErrParameterTypeMistmatch,
		fmt.Sprintf("[%d:%d] an expression of type '%s' cannot be passed to a parameter of type '%s'", line, col, type1, type2),
	)
}

func NewErrCallParameterValueInvalid(line, col int, badParameterValue string, parameterName string) error {
	return newError(
		ErrCallParameterValueInvalid,
		fmt.Sprintf("[%d:%d] invalid value '%s' for parameter '%s'", line, col, badParameterValue, parameterName),
	)
}

// insert

func NewErrInsertValueOutOfRange(line, col int, columnName string, rowNumber int, badValue interface{}) error {
	return newError(
		ErrInsertValueOutOfRange,
		fmt.Sprintf("[%d:%d] inserting value into column '%s', row %d, value '%v' out of range", line, col, columnName, rowNumber, badValue),
	)
}

func NewErrUnexpectedTimeQuantumTupleLength(line, col int, columnName string, rowNumber int, badValue []interface{}, length int) error {
	return newError(
		ErrUnexpectedTimeQuantumTupleLength,
		fmt.Sprintf("[%d:%d] inserting value into column '%s', row %d, value '%v' out of range", line, col, columnName, rowNumber, badValue),
	)
}

// bulk insert

func NewErrReadingDatasource(line, col int, dataSource string, errorText string) error {
	return newError(
		ErrReadingDatasource,
		fmt.Sprintf("[%d:%d] unable to read datasource '%s': %s", line, col, dataSource, errorText),
	)
}

func NewErrMappingFromDatasource(line, col int, dataSource string, errorText string) error {
	return newError(
		ErrMappingFromDatasource,
		fmt.Sprintf("[%d:%d] unable to map from datasource '%s': %s", line, col, dataSource, errorText),
	)
}

func NewErrFormatSpecifierExpected(line, col int) error {
	return newError(
		ErrFormatSpecifierExpected,
		fmt.Sprintf("[%d:%d] format specifier expected", line, col),
	)
}

func NewErrInvalidFormatSpecifier(line, col int, specifier string) error {
	return newError(
		ErrInvalidFormatSpecifier,
		fmt.Sprintf("[%d:%d] invalid format specifier '%s'", line, col, specifier),
	)
}

func NewErrInputSpecifierExpected(line, col int) error {
	return newError(
		ErrInputSpecifierExpected,
		fmt.Sprintf("[%d:%d] input specifier expected", line, col),
	)
}

func NewErrInvalidInputSpecifier(line, col int, specifier string) error {
	return newError(
		ErrInvalidFormatSpecifier,
		fmt.Sprintf("[%d:%d] invalid input specifier '%s'", line, col, specifier),
	)
}

func NewErrInvalidBatchSize(line, col int, batchSize int) error {
	return newError(
		ErrInvalidBatchSize,
		fmt.Sprintf("[%d:%d] invalid batch size '%d'", line, col, batchSize),
	)
}

func NewErrTypeConversionOnMap(line, col int, value interface{}, typeName string) error {
	return newError(
		ErrTypeConversionOnMap,
		fmt.Sprintf("[%d:%d] value '%v' cannot be converted to type '%s'", line, col, value, typeName),
	)
}

func NewErrParsingJSON(line, col int, jsonString string, errorText string) error {
	return newError(
		ErrParsingJSON,
		fmt.Sprintf("[%d:%d] unable to parse JSON '%s': %s", line, col, jsonString, errorText),
	)
}

func NewErrEvaluatingJSONPathExpr(line, col int, exprText string, jsonString string, errorText string) error {
	return newError(
		ErrEvaluatingJSONPathExpr,
		fmt.Sprintf("[%d:%d] unable to evaluate JSONPath expression '%s' in '%s': %s", line, col, exprText, jsonString, errorText),
	)
}

// optimizer

func NewErrAggregateNotAllowedInGroupBy(line, col int, aggName string) error {
	return newError(
		ErrAggregateNotAllowedInGroupBy,
		fmt.Sprintf("[%d:%d] aggregate '%s' not allowed in GROUP BY", line, col, aggName),
	)
}

// function evaluation
func NewErrValueOutOfRange(line, col int, val interface{}) error {
	return newError(
		ErrValueOutOfRange,
		fmt.Sprintf("[%d:%d] value '%v' out of range", line, col, val),
	)
}

func NewErrStringLengthMismatch(line, col, len int, val interface{}) error {
	return newError(
		ErrStringLengthMismatch,
		fmt.Sprintf("[%d:%d] value '%v' should be of the length %d", line, col, val, len),
	)
}

func NewErrUnexpectedTypeConversion(line, col int, val interface{}) error {
	return newError(
		ErrUnexpectedTypeConversion,
		NewErrInternalf("unexpected type conversion %T", val).Error(),
	)
}

// time quantum function evaluation

func NewErrQRangeFromAndToTimeCannotBeBothNull(line, col int) error {
	return newError(
		ErrQRangeFromAndToTimeCannotBeBothNull,
		fmt.Sprintf("[%d:%d] calling ranqeq() 'from' and 'to' parameters cannot both be null", line, col),
	)
}

func NewErrQRangeInvalidUse(line, col int) error {
	return newError(
		ErrQRangeInvalidUse,
		fmt.Sprintf("[%d:%d] calling ranqeq() usage invalid", line, col),
	)
}

func NewErrInvalidDatetimePart(line, col int, datetimepart int) error {
	return newError(
		ErrInvalidDatetimePart,
		fmt.Sprintf("[%d:%d] not a valid datetimepart %d", line, col, datetimepart),
	)
}

func NewErrOutputValueOutOfRange(line, col int) error {
	return newError(
		ErrOutputValueOutOfRange,
		fmt.Sprintf("[%d:%d] output value out of range", line, col),
	)
}

func NewErrDivideByZero(line, col int) error {
	return newError(
		ErrDivideByZero,
		fmt.Sprintf("[%d:%d] divisor is equal to zero", line, col),
	)
}

func NewErrRemoteUnauthorized(line, col int, remoteUrl string) error {
	return newError(
		ErrRemoteUnauthorized,
		fmt.Sprintf("unauthorized on remote server '%s'", remoteUrl),
	)
}

// query hints

func NewErrUnknownQueryHint(line, col int, hintName string) error {
	return newError(
		ErrUnknownQueryHint,
		fmt.Sprintf("[%d:%d] unknown query hint '%s'", line, col, hintName),
	)
}

func NewErrInvalidQueryHintParameterCount(line, col int, hintName string, desiredList string, desiredCount int, actualCount int) error {
	return newError(
		ErrInvalidQueryHintParameterCount,
		fmt.Sprintf("[%d:%d] query hint '%s' expected %d parameter(s) (%s), got %d parameters", line, col, hintName, desiredCount, desiredList, actualCount),
	)
}

// show options

func NewErrUnknownShowOption(line, col int, optionName string) error {
	return newError(
		ErrUnknownShowOption,
		fmt.Sprintf("[%d:%d] unknown show option '%s'", line, col, optionName),
	)
}
