-- 修复 binlog 校验和问题
-- 运行这个脚本后重新运行 binlog_slave_protocol.go

-- 1. 查看当前的 binlog 校验和配置
SHOW VARIABLES LIKE 'binlog_checksum';

-- 2. 禁用 binlog 校验和 (推荐用于测试)
SET GLOBAL binlog_checksum=NONE;

-- 3. 验证已禁用
SHOW VARIABLES LIKE 'binlog_checksum';

-- 注意：
-- - 禁用校验和后，binlog 文件不会自动验证
-- - 这是测试环境，生产环境可能需要启用校验和
-- - 如果要启用校验和，需要在代码中支持校验和解析
