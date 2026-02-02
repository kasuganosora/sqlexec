package api

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Execute executes an INSERT, UPDATE, or DELETE statement and returns number of affected rows
// Supports parameter binding with ? placeholders
// For SELECT, SHOW, DESCRIBE, and EXPLAIN statements, use Query() or Explain() method instead
func (s *Session) Execute(sql string, args ...interface{}) (*Result, error) {
	s.mu.RLock()
	if s.err != nil {
		s.mu.RUnlock()
		return nil, s.err
	}
	s.mu.RUnlock()

	// Bind parameters if provided
	boundSQL := sql
	if len(args) > 0 {
		var err error
		boundSQL, err = bindParams(sql, args)
		if err != nil {
			return nil, WrapError(err, ErrCodeInvalidParam, "failed to bind parameters")
		}
	}

	s.logger.Debug("Execute: %s", boundSQL)

	// Parse SQL to determine statement type
	parseResult, err := s.coreSession.GetAdapter().Parse(boundSQL)
	if err != nil {
		return nil, WrapError(err, ErrCodeSyntax, "failed to parse SQL")
	}

	if !parseResult.Success {
		return nil, NewError(ErrCodeSyntax, "SQL parse error: "+parseResult.Error, nil)
	}

	// Check for read-only information_schema operations (check both SQL string and parsed statement)
	if s.isInformationSchemaOperation(boundSQL, parseResult.Statement) {
		s.logger.Debug("Blocking information_schema DML operation")
		return nil, NewError(ErrCodeInternal, "information_schema is read-only: DML operations are not supported", nil)
	}

	ctx := context.Background()
	var result *domain.QueryResult

	switch parseResult.Statement.Type {
	case parser.SQLTypeInsert:
		result, err = s.coreSession.ExecuteInsert(ctx, boundSQL, nil)
	case parser.SQLTypeUpdate:
		result, err = s.coreSession.ExecuteUpdate(ctx, boundSQL, nil, nil)
	case parser.SQLTypeDelete:
		result, err = s.coreSession.ExecuteDelete(ctx, boundSQL, nil)
	case parser.SQLTypeCreate:
		// 优先处理 CREATE INDEX
		if parseResult.Statement.CreateIndex != nil {
			result, err = s.coreSession.ExecuteCreateIndex(ctx, boundSQL)
		} else {
			result, err = s.coreSession.ExecuteCreate(ctx, boundSQL)
		}
	case parser.SQLTypeDrop:
		// 优先处理 DROP INDEX
		if parseResult.Statement.DropIndex != nil {
			result, err = s.coreSession.ExecuteDropIndex(ctx, boundSQL)
		} else {
			result, err = s.coreSession.ExecuteDrop(ctx, boundSQL)
		}
	case parser.SQLTypeAlter:
		result, err = s.coreSession.ExecuteAlter(ctx, boundSQL)
	case parser.SQLTypeUse:
		result, err = s.coreSession.ExecuteQuery(ctx, boundSQL)
	case parser.SQLTypeSelect, parser.SQLTypeShow, parser.SQLTypeDescribe, parser.SQLTypeExplain:
		return nil, NewError(ErrCodeInvalidParam, fmt.Sprintf("use Query() method for %s statements (or Explain() for EXPLAIN)", parseResult.Statement.Type), nil)
	default:
		return nil, NewError(ErrCodeNotSupported, fmt.Sprintf("unsupported statement type: %v", parseResult.Statement.Type), nil)
	}

	if err != nil {
		// 检查错误类型并返回适当的错误码
		if err.Error() == "query execution timed out" || err.Error() == "query was killed" {
			return nil, WrapError(err, ErrCodeTimeout, "failed to execute statement")
		}
		return nil, WrapError(err, ErrCodeInternal, "failed to execute statement")
	}

	// Clear cache for affected table
	if s.cacheEnabled {
		var tableName string
		switch parseResult.Statement.Type {
		case parser.SQLTypeInsert:
			tableName = parseResult.Statement.Insert.Table
		case parser.SQLTypeUpdate:
			tableName = parseResult.Statement.Update.Table
		case parser.SQLTypeDelete:
			tableName = parseResult.Statement.Delete.Table
		case parser.SQLTypeCreate:
			if parseResult.Statement.CreateIndex != nil {
				tableName = parseResult.Statement.CreateIndex.TableName
			} else {
				tableName = parseResult.Statement.Create.Name
			}
		case parser.SQLTypeDrop:
			if parseResult.Statement.DropIndex != nil {
				tableName = parseResult.Statement.DropIndex.TableName
			} else {
				tableName = parseResult.Statement.Drop.Name
			}
		}
		if tableName != "" {
			s.db.cache.ClearTable(tableName)
		}
	}

	return NewResult(result.Total, 0, nil), nil
}
