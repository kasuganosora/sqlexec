package generated

import (
	"testing"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestDebugFullCalculation(t *testing.T) {
	calc := NewVirtualCalculator()
	
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "price", Type: "DECIMAL", Nullable: false},
			{Name: "quantity", Type: "INT", Nullable: false},
			{
				Name:          "total",
				Type:          "DECIMAL",
				Nullable:      false,
				IsGenerated:   true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "price * quantity",
				GeneratedDepends: []string{"price", "quantity"},
			},
		},
	}
	
	row := domain.Row{"id": int64(1), "price": 10.5, "quantity": int64(2)}
	
	fmt.Printf("=== Step 1: Get Evaluation Order ===\n")
	order, err := calc.getEvaluationOrder(schema)
	fmt.Printf("Order: %v, Error: %v\n", order, err)
	
	fmt.Printf("\n=== Step 2: Check total column info ===\n")
	totalCol := calc.getColumnInfo("total", schema)
	if totalCol != nil {
		fmt.Printf("Total column: %+v\n", *totalCol)
		fmt.Printf("IsGenerated: %v\n", totalCol.IsGenerated)
		fmt.Printf("GeneratedType: %s\n", totalCol.GeneratedType)
	} else {
		fmt.Printf("Total column not found!\n")
	}
	
	fmt.Printf("\n=== Step 3: CalculateColumn directly ===\n")
	if totalCol != nil {
		result, err := calc.CalculateColumn(totalCol, row, schema)
		fmt.Printf("Result: %v, Error: %v\n", result, err)
	}
	
	fmt.Printf("\n=== Step 4: CalculateRowVirtuals ===\n")
	resultRow, err := calc.CalculateRowVirtuals(row, schema)
	fmt.Printf("Result row: %+v\n", resultRow)
	fmt.Printf("Error: %v\n", err)
	fmt.Printf("Has total: %v\n", resultRow["total"] != nil)
}
