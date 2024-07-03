package planner

import (
	"github.com/gernest/rbf/ql/sql3"
)

func (n *callPlanExpression) evaluateUserDefinedFunction(currentRow []interface{}) (interface{}, error) {
	// TODO(pok) removing what effectively is a remote code exploit
	// we will come back to this to add sql udfs and external code later

	// argEval, err := n.args[0].Evaluate(currentRow)
	// if err != nil {
	// 	return nil, err
	// }
	// // nil if anything is nil
	// if argEval == nil {
	// 	return nil, nil
	// }

	// //get the value
	// coercedArg, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), argEval, parser.Pos{Line: 0, Column: 0})
	// if err != nil {
	// 	return nil, err
	// }

	// arg, argOk := coercedArg.(string)
	// if !argOk {
	// 	return nil, sql3.NewErrInternalf("unable to convert value")
	// }

	// // save the body to a temp file
	// file, err := os.CreateTemp("", "py-body")
	// if err != nil {
	// 	return nil, err
	// }
	// defer os.Remove(file.Name())

	// file.Write([]byte(n.udfReference.body))

	// cmd := exec.Command("python3", file.Name(), arg)
	// stdout, err := cmd.Output()

	// if err != nil {
	// 	return nil, err
	// }

	// retVal := string(stdout)
	// return retVal, nil
	return nil, sql3.NewErrUnsupported(0, 0, false, "user defined functions")
}
