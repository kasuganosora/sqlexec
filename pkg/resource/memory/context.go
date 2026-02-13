package memory

import "context"

// TransactionIDKey is the context key for transaction ID
type TransactionIDKey struct{}

// GetTransactionID retrieves transaction ID from context
func GetTransactionID(ctx context.Context) (int64, bool) {
	txnID, ok := ctx.Value(TransactionIDKey{}).(int64)
	return txnID, ok
}

// SetTransactionID sets transaction ID into context
func SetTransactionID(ctx context.Context, txnID int64) context.Context {
	return context.WithValue(ctx, TransactionIDKey{}, txnID)
}
