package main

import (
	"fmt"
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func main() {
	adapter := parser.NewSQLAdapter()
	result, err := adapter.Parse("SELECT * FROM information_schema.schemata")
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	if !result.Success {
		fmt.Printf("Parse failed: %s\n", result.Error)
		return
	}
	
	fmt.Printf("Statement Type: %v\n", result.Statement.Type)
	if result.Statement.Select != nil {
		fmt.Printf("From: %s\n", result.Statement.Select.From)
	}
}
