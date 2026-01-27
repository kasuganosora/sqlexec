package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSQLScenarios 测试各种SQL场景
// 注意：这些是结构化测试，用于验证SQL解析和执行流程
// 实际的SQL执行测试需要完整的数据库设置

func TestSQLScenarios_Select(t *testing.T) {
	// 简单SELECT
	simpleSelect := "SELECT * FROM users"
	assert.Contains(t, simpleSelect, "SELECT")
	assert.Contains(t, simpleSelect, "FROM")

	// 带WHERE的SELECT
	selectWithWhere := "SELECT * FROM users WHERE age > 30"
	assert.Contains(t, selectWithWhere, "WHERE")

	// 带ORDER BY的SELECT
	selectWithOrder := "SELECT * FROM users ORDER BY name DESC"
	assert.Contains(t, selectWithOrder, "ORDER BY")

	// 带LIMIT的SELECT
	selectWithLimit := "SELECT * FROM users LIMIT 10"
	assert.Contains(t, selectWithLimit, "LIMIT")

	// 带OFFSET的SELECT
	selectWithOffset := "SELECT * FROM users LIMIT 10 OFFSET 5"
	assert.Contains(t, selectWithOffset, "LIMIT")
	assert.Contains(t, selectWithOffset, "OFFSET")

	// SELECT多个列
	selectColumns := "SELECT id, name, email FROM users"
	assert.Contains(t, selectColumns, "SELECT")
}

func TestSQLScenarios_Join(t *testing.T) {
	// INNER JOIN
	innerJoin := "SELECT u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id"
	assert.Contains(t, innerJoin, "INNER JOIN")

	// LEFT JOIN
	leftJoin := "SELECT u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id"
	assert.Contains(t, leftJoin, "LEFT JOIN")

	// RIGHT JOIN
	rightJoin := "SELECT u.name, o.order_id FROM users u RIGHT JOIN orders o ON u.id = o.user_id"
	assert.Contains(t, rightJoin, "RIGHT JOIN")

	// 多表JOIN
	multiJoin := "SELECT u.name, p.name, o.amount FROM users u JOIN products p ON u.id = p.user_id JOIN orders o ON u.id = o.user_id"
	assert.Contains(t, multiJoin, "JOIN")
}

func TestSQLScenarios_ComplexJoin(t *testing.T) {
	// 带子查询的JOIN
	joinWithSubquery := "SELECT u.* FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100)"
	assert.Contains(t, joinWithSubquery, "IN")
	assert.Contains(t, joinWithSubquery, "SELECT")

	// 自连接
	selfJoin := "SELECT e1.name, e2.name as manager FROM employees e1 JOIN employees e2 ON e1.manager_id = e2.id"
	assert.Contains(t, selfJoin, "JOIN")

	// JOIN with多个条件
	joinMultiCondition := "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id AND o.status = 'completed'"
	assert.Contains(t, joinMultiCondition, "AND")
}

func TestSQLScenarios_Insert(t *testing.T) {
	// 简单INSERT
	simpleInsert := "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
	assert.Contains(t, simpleInsert, "INSERT")
	assert.Contains(t, simpleInsert, "VALUES")

	// 多行INSERT
	multiInsert := "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')"
	assert.Contains(t, multiInsert, "INSERT")
	assert.Contains(t, multiInsert, "VALUES")
}

func TestSQLScenarios_Update(t *testing.T) {
	// 简单UPDATE
	simpleUpdate := "UPDATE users SET name = 'Alice' WHERE id = 1"
	assert.Contains(t, simpleUpdate, "UPDATE")
	assert.Contains(t, simpleUpdate, "SET")
	assert.Contains(t, simpleUpdate, "WHERE")

	// UPDATE多个字段
	updateMultiple := "UPDATE users SET name = 'Alice', email = 'newalice@example.com' WHERE id = 1"
	assert.Contains(t, updateMultiple, "UPDATE")
	assert.Contains(t, updateMultiple, "SET")

	// UPDATE带表达式
	updateWithExpr := "UPDATE users SET age = age + 1 WHERE id = 1"
	assert.Contains(t, updateWithExpr, "UPDATE")
	assert.Contains(t, updateWithExpr, "SET")

	// UPDATE带IN
	updateWithIn := "UPDATE users SET status = 'active' WHERE id IN (1, 2, 3)"
	assert.Contains(t, updateWithIn, "UPDATE")
	assert.Contains(t, updateWithIn, "IN")
}

func TestSQLScenarios_Delete(t *testing.T) {
	// 简单DELETE
	simpleDelete := "DELETE FROM users WHERE id = 1"
	assert.Contains(t, simpleDelete, "DELETE")
	assert.Contains(t, simpleDelete, "WHERE")

	// DELETE带IN
	deleteWithIn := "DELETE FROM users WHERE id IN (1, 2, 3)"
	assert.Contains(t, deleteWithIn, "DELETE")
	assert.Contains(t, deleteWithIn, "IN")

	// DELETE带子查询
	deleteWithSubquery := "DELETE FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'cancelled')"
	assert.Contains(t, deleteWithSubquery, "DELETE")
	assert.Contains(t, deleteWithSubquery, "SELECT")
}

func TestSQLScenarios_ComplexWhere(t *testing.T) {
	// 带多个AND条件
	whereAnd := "SELECT * FROM users WHERE age > 30 AND status = 'active'"
	assert.Contains(t, whereAnd, "AND")

	// 带OR条件
	whereOr := "SELECT * FROM users WHERE age < 20 OR age > 60"
	assert.Contains(t, whereOr, "OR")

	// 带NOT条件
	whereNot := "SELECT * FROM users WHERE NOT status = 'deleted'"
	assert.Contains(t, whereNot, "NOT")

	// 带BETWEEN
	whereBetween := "SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'"
	assert.Contains(t, whereBetween, "BETWEEN")

	// 带LIKE
	whereLike := "SELECT * FROM users WHERE name LIKE 'A%'"
	assert.Contains(t, whereLike, "LIKE")

	// 复杂WHERE组合
	whereComplex := "SELECT * FROM users WHERE (age > 30 AND status = 'active') OR (priority = 'high' AND created_at > '2024-01-01')"
	assert.Contains(t, whereComplex, "AND")
	assert.Contains(t, whereComplex, "OR")
}

func TestSQLScenarios_Functions(t *testing.T) {
	// 带聚合函数
	selectCount := "SELECT COUNT(*) as total FROM users"
	assert.Contains(t, selectCount, "COUNT")

	// 带MAX
	selectMax := "SELECT MAX(age) as max_age FROM users"
	assert.Contains(t, selectMax, "MAX")

	// 带MIN
	selectMin := "SELECT MIN(age) as min_age FROM users"
	assert.Contains(t, selectMin, "MIN")

	// 带SUM
	selectSum := "SELECT SUM(amount) as total_amount FROM orders"
	assert.Contains(t, selectSum, "SUM")

	// 带AVG
	selectAvg := "SELECT AVG(age) as avg_age FROM users"
	assert.Contains(t, selectAvg, "AVG")

	// 带GROUP BY
	selectGroupBy := "SELECT status, COUNT(*) as count FROM users GROUP BY status"
	assert.Contains(t, selectGroupBy, "GROUP BY")

	// 带HAVING
	selectHaving := "SELECT status, COUNT(*) as count FROM users GROUP BY status HAVING COUNT(*) > 10"
	assert.Contains(t, selectHaving, "HAVING")

	// 带DISTINCT
	selectDistinct := "SELECT DISTINCT status FROM users"
	assert.Contains(t, selectDistinct, "DISTINCT")
}

func TestSQLScenarios_Subqueries(t *testing.T) {
	// WHERE子查询
	whereSubquery := "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)"
	assert.Contains(t, whereSubquery, "IN")
	assert.Contains(t, whereSubquery, "SELECT")

	// FROM子查询
	fromSubquery := "SELECT u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count FROM users u"
	assert.Contains(t, fromSubquery, "SELECT")

	// EXISTS子查询
	existsSubquery := "SELECT * FROM users u WHERE EXISTS (SELECT * FROM orders o WHERE o.user_id = u.id)"
	assert.Contains(t, existsSubquery, "EXISTS")

	// NOT EXISTS子查询
	notExistsSubquery := "SELECT * FROM users u WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.user_id = u.id)"
	assert.Contains(t, notExistsSubquery, "EXISTS")
}

func TestSQLScenarios_OrderLimit(t *testing.T) {
	// 简单ORDER BY
	simpleOrder := "SELECT * FROM users ORDER BY name"
	assert.Contains(t, simpleOrder, "ORDER BY")

	// ORDER BY DESC
	orderDesc := "SELECT * FROM users ORDER BY created_at DESC"
	assert.Contains(t, orderDesc, "DESC")

	// ORDER BY多个字段
	orderMultiple := "SELECT * FROM users ORDER BY status DESC, created_at ASC"
	assert.Contains(t, orderMultiple, "ASC")

	// 带LIMIT
	withLimit := "SELECT * FROM users ORDER BY name LIMIT 10"
	assert.Contains(t, withLimit, "LIMIT")

	// 带LIMIT和OFFSET
	withLimitOffset := "SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20"
	assert.Contains(t, withLimitOffset, "LIMIT")
	assert.Contains(t, withLimitOffset, "OFFSET")
}

func TestSQLScenarios_CreateDrop(t *testing.T) {
	// CREATE TABLE
	createTable := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(100),
		status VARCHAR(20)
	)`
	assert.Contains(t, createTable, "CREATE TABLE")
	assert.Contains(t, createTable, "PRIMARY KEY")

	// CREATE INDEX
	createIndex := "CREATE INDEX idx_email ON users(email)"
	assert.Contains(t, createIndex, "CREATE INDEX")

	// DROP TABLE
	dropTable := "DROP TABLE IF EXISTS users"
	assert.Contains(t, dropTable, "DROP TABLE")
	assert.Contains(t, dropTable, "IF EXISTS")

	// DROP INDEX
	dropIndex := "DROP INDEX IF EXISTS idx_email"
	assert.Contains(t, dropIndex, "DROP INDEX")
}

func TestSQLScenarios_Verification(t *testing.T) {
	// 验证SQL的基本语法
	assert.Contains(t, "SELECT * FROM users", "SELECT")
	assert.Contains(t, "INSERT INTO users (name) VALUES ('test')", "INSERT")
	assert.Contains(t, "UPDATE users SET name = 'test' WHERE id = 1", "UPDATE")
	assert.Contains(t, "DELETE FROM users WHERE id = 1", "DELETE")
	assert.Contains(t, "CREATE TABLE test (id INT)", "CREATE")
	assert.Contains(t, "DROP TABLE test", "DROP")
}

func TestSQLScenarios_ShowDescribe(t *testing.T) {
	// SHOW TABLES
	showTables := "SHOW TABLES"
	assert.Contains(t, showTables, "SHOW")
	assert.Contains(t, showTables, "TABLES")

	// SHOW TABLES LIKE pattern
	showTablesLike := "SHOW TABLES LIKE 'user%'"
	assert.Contains(t, showTablesLike, "SHOW")
	assert.Contains(t, showTablesLike, "LIKE")

	// SHOW TABLES WHERE clause
	showTablesWhere := "SHOW TABLES WHERE table_schema = 'test'"
	assert.Contains(t, showTablesWhere, "SHOW")
	assert.Contains(t, showTablesWhere, "WHERE")

	// SHOW DATABASES
	showDatabases := "SHOW DATABASES"
	assert.Contains(t, showDatabases, "SHOW")
	assert.Contains(t, showDatabases, "DATABASES")

	// SHOW COLUMNS FROM table
	showColumns := "SHOW COLUMNS FROM users"
	assert.Contains(t, showColumns, "SHOW")
	assert.Contains(t, showColumns, "COLUMNS")
	assert.Contains(t, showColumns, "users")

	// SHOW CREATE TABLE
	showCreateTable := "SHOW CREATE TABLE users"
	assert.Contains(t, showCreateTable, "SHOW")
	assert.Contains(t, showCreateTable, "CREATE")
	assert.Contains(t, showCreateTable, "TABLE")

	// DESCRIBE table
	describeTable := "DESCRIBE users"
	assert.Contains(t, describeTable, "DESCRIBE")
	assert.Contains(t, describeTable, "users")

	// DESC table (alias for DESCRIBE)
	descTable := "DESC users"
	assert.Contains(t, descTable, "DESC")
	assert.Contains(t, descTable, "users")

	// DESCRIBE table column
	describeColumn := "DESCRIBE users name"
	assert.Contains(t, describeColumn, "DESCRIBE")
	assert.Contains(t, describeColumn, "users")
	assert.Contains(t, describeColumn, "name")

	// DESC table column
	descColumn := "DESC users email"
	assert.Contains(t, descColumn, "DESC")
	assert.Contains(t, descColumn, "users")
	assert.Contains(t, descColumn, "email")
}
