package api

// Session represents a database session (like a MySQL connection)
// It is concurrent safe and can be used across multiple goroutines
//
// The Session type provides high-level database operations including:
//   - Query operations: Query(), QueryAll(), QueryOne()
//   - DML operations: Execute()
//   - Transaction management: Begin(), InTransaction(), IsolationLevel(), SetIsolationLevel()
//   - Temporary table management: CreateTempTable()
//   - Session lifecycle: GetDB(), Close()
//   - Query timeout and kill support
//
// Session is created by DB.Session() or DB.SessionWithOptions()
// and wraps CoreSession with additional functionality like caching and validation.
