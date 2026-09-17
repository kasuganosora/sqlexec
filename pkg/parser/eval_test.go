package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvalExpr(t *testing.T) {
	row := Row{
		"gold": int64(1000),
		"qty":  int64(10),
		"rate": 1.5,
	}

	tests := []struct {
		name    string
		expr    *Expression
		want    interface{}
		wantErr string
	}{
		{
			name: "nil expression",
			expr: nil,
			want: nil,
		},
		{
			name: "literal value",
			expr: &Expression{Type: ExprTypeValue, Value: int64(42)},
			want: int64(42),
		},
		{
			name: "column reference",
			expr: &Expression{Type: ExprTypeColumn, Column: "gold"},
			want: int64(1000),
		},
		{
			name: "qualified column reference",
			expr: &Expression{Type: ExprTypeColumn, Column: "t.gold"},
			want: int64(1000),
		},
		{
			name:    "missing column",
			expr:    &Expression{Type: ExprTypeColumn, Column: "missing"},
			wantErr: "not found",
		},
		{
			name: "subtract column minus literal",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "-",
				Left:     &Expression{Type: ExprTypeColumn, Column: "gold"},
				Right:    &Expression{Type: ExprTypeValue, Value: int64(300)},
			},
			want: int64(700),
		},
		{
			name: "add",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "+",
				Left:     &Expression{Type: ExprTypeColumn, Column: "qty"},
				Right:    &Expression{Type: ExprTypeValue, Value: int64(3)},
			},
			want: int64(13),
		},
		{
			name: "multiply",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "*",
				Left:     &Expression{Type: ExprTypeColumn, Column: "qty"},
				Right:    &Expression{Type: ExprTypeValue, Value: int64(2)},
			},
			want: int64(20),
		},
		{
			name: "divide",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "/",
				Left:     &Expression{Type: ExprTypeColumn, Column: "gold"},
				Right:    &Expression{Type: ExprTypeValue, Value: int64(4)},
			},
			want: float64(250),
		},
		{
			name: "modulo",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "%",
				Left:     &Expression{Type: ExprTypeColumn, Column: "gold"},
				Right:    &Expression{Type: ExprTypeValue, Value: int64(300)},
			},
			want: int64(100),
		},
		{
			name: "nested arithmetic",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "+",
				Left: &Expression{
					Type:     ExprTypeOperator,
					Operator: "-",
					Left:     &Expression{Type: ExprTypeColumn, Column: "gold"},
					Right:    &Expression{Type: ExprTypeValue, Value: int64(300)},
				},
				Right: &Expression{Type: ExprTypeValue, Value: int64(50)},
			},
			want: int64(750),
		},
		{
			name: "float divide uses float path",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "/",
				Left:     &Expression{Type: ExprTypeColumn, Column: "rate"},
				Right:    &Expression{Type: ExprTypeValue, Value: 2.0},
			},
			want: 0.75,
		},
		{
			name: "unary minus",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "-",
				Left:     &Expression{Type: ExprTypeValue, Value: int64(7)},
			},
			want: int64(-7),
		},
		{
			name: "unary plus",
			expr: &Expression{
				Type:     ExprTypeOperator,
				Operator: "+",
				Left:     &Expression{Type: ExprTypeColumn, Column: "qty"},
			},
			want: int64(10),
		},
		{
			name:    "division by zero",
			expr:    &Expression{Type: ExprTypeOperator, Operator: "/", Left: &Expression{Type: ExprTypeValue, Value: int64(1)}, Right: &Expression{Type: ExprTypeValue, Value: int64(0)}},
			wantErr: "division by zero",
		},
		{
			name:    "modulo by zero",
			expr:    &Expression{Type: ExprTypeOperator, Operator: "%", Left: &Expression{Type: ExprTypeValue, Value: int64(1)}, Right: &Expression{Type: ExprTypeValue, Value: int64(0)}},
			wantErr: "division by zero",
		},
		{
			name:    "unsupported operator",
			expr:    &Expression{Type: ExprTypeOperator, Operator: "&", Left: &Expression{Type: ExprTypeValue, Value: int64(1)}, Right: &Expression{Type: ExprTypeValue, Value: int64(2)}},
			wantErr: "unsupported arithmetic operator",
		},
		{
			name:    "unsupported expression type",
			expr:    &Expression{Type: ExprTypeFunction, Function: "abs"},
			wantErr: "unsupported expression type",
		},
		{
			name:    "missing left operand",
			expr:    &Expression{Type: ExprTypeOperator, Operator: "+"},
			wantErr: "missing left operand",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalExpr(tt.expr, row)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseUpdateSetArithmetic(t *testing.T) {
	adapter := NewSQLAdapter()
	result, err := adapter.Parse("UPDATE inventory SET gold = gold - 300 WHERE id = 1")
	require.NoError(t, err)
	require.True(t, result.Success, result.Error)
	require.Equal(t, SQLTypeUpdate, result.Statement.Type)
	require.NotNil(t, result.Statement.Update)

	raw, ok := result.Statement.Update.Set["gold"]
	require.True(t, ok, "SET clause should include gold")
	expr, ok := raw.(*Expression)
	require.True(t, ok, "gold assignment should be an expression, got %T", raw)
	assert.Equal(t, ExprTypeOperator, expr.Type)
	assert.Contains(t, []string{"-", "minus"}, expr.Operator)

	row := Row{"gold": int64(1000)}
	got, err := EvalExpr(expr, row)
	require.NoError(t, err)
	assert.Equal(t, int64(700), got)
}
