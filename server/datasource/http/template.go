package http

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TemplateContext 模板渲染上下文
type TemplateContext struct {
	Method    string // HTTP 方法 (GET/POST)
	Path      string // 请求路径
	Body      string // 请求 body
	AuthToken string // 配置中的 auth_token
}

// RenderTemplate 渲染模板字符串，替换 {{变量}} 和 {{函数(参数)}}
func RenderTemplate(template string, ctx *TemplateContext) string {
	now := time.Now()
	nonce := generateNonce()
	uuidVal := uuid.New().String()

	// 预先计算所有变量值
	vars := map[string]string{
		"timestamp":    fmt.Sprintf("%d", now.Unix()),
		"timestamp_ms": fmt.Sprintf("%d", now.UnixMilli()),
		"uuid":         uuidVal,
		"nonce":        nonce,
		"date":         now.Format("2006-01-02"),
		"datetime":     now.UTC().Format("2006-01-02T15:04:05Z"),
		"method":       ctx.Method,
		"path":         ctx.Path,
		"body":         ctx.Body,
		"auth_token":   ctx.AuthToken,
	}

	result := template
	// 持续替换直到没有更多 {{ }} 模板
	for i := 0; i < 10; i++ { // 防止无限循环
		start := strings.Index(result, "{{")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "}}")
		if end == -1 {
			break
		}
		end += start + 2

		expr := result[start+2 : end-2]
		expr = strings.TrimSpace(expr)

		replacement := evaluateExpression(expr, vars)
		result = result[:start] + replacement + result[end:]
	}

	return result
}

// evaluateExpression 解析并执行表达式
// 支持：变量名、函数调用 func(arg1, arg2)
func evaluateExpression(expr string, vars map[string]string) string {
	// 检查是否是函数调用: func(args)
	parenStart := strings.Index(expr, "(")
	if parenStart > 0 && strings.HasSuffix(expr, ")") {
		funcName := strings.TrimSpace(expr[:parenStart])
		argsStr := expr[parenStart+1 : len(expr)-1]
		args := parseArgs(argsStr, vars)
		return callFunction(funcName, args)
	}

	// 否则是变量引用
	if val, ok := vars[expr]; ok {
		return val
	}
	return ""
}

// parseArgs 解析函数参数列表
// 参数之间用逗号分隔，每个参数内部支持 + 拼接
func parseArgs(argsStr string, vars map[string]string) []string {
	parts := splitArgs(argsStr)
	result := make([]string, len(parts))
	for i, part := range parts {
		result[i] = resolveArg(strings.TrimSpace(part), vars)
	}
	return result
}

// splitArgs 按逗号分隔参数（考虑嵌套括号）
func splitArgs(s string) []string {
	var result []string
	depth := 0
	current := strings.Builder{}
	for _, ch := range s {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

// resolveArg 解析单个参数值，支持 + 拼接
func resolveArg(arg string, vars map[string]string) string {
	parts := strings.Split(arg, "+")
	var sb strings.Builder
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// 检查是否是字面字符串（用引号包裹）
		if (strings.HasPrefix(p, "\"") && strings.HasSuffix(p, "\"")) ||
			(strings.HasPrefix(p, "'") && strings.HasSuffix(p, "'")) {
			sb.WriteString(p[1 : len(p)-1])
		} else if val, ok := vars[p]; ok {
			sb.WriteString(val)
		} else {
			sb.WriteString(p)
		}
	}
	return sb.String()
}

// callFunction 执行内置函数
func callFunction(name string, args []string) string {
	switch name {
	case "hmac_sha256":
		if len(args) < 2 {
			return ""
		}
		return hmacSHA256(args[0], args[1])
	case "hmac_md5":
		if len(args) < 2 {
			return ""
		}
		return hmacMD5(args[0], args[1])
	case "md5":
		if len(args) < 1 {
			return ""
		}
		return hashMD5(args[0])
	case "sha256":
		if len(args) < 1 {
			return ""
		}
		return hashSHA256(args[0])
	case "base64":
		if len(args) < 1 {
			return ""
		}
		return base64Encode(args[0])
	case "upper":
		if len(args) < 1 {
			return ""
		}
		return strings.ToUpper(args[0])
	case "lower":
		if len(args) < 1 {
			return ""
		}
		return strings.ToLower(args[0])
	default:
		return ""
	}
}

// ── 内置签名/哈希函数 ──

func hmacSHA256(key, data string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func hmacMD5(key, data string) string {
	mac := hmac.New(md5.New, []byte(key))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func hashMD5(data string) string {
	h := md5.Sum([]byte(data))
	return hex.EncodeToString(h[:])
}

func hashSHA256(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func base64Encode(data string) string {
	return base64.StdEncoding.EncodeToString([]byte(data))
}

func generateNonce() string {
	u := uuid.New()
	b := u[:]
	return hex.EncodeToString(b[:8])
}
