package dsl

import "fmt"

func ViewKey(field, view string) string {
	return fmt.Sprintf("~%s;%s<", field, view)
}
