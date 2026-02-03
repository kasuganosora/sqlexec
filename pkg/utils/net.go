package utils

// ParseRemoteAddr 解析远程地址，格式为 "ip:port"
// 返回 (ip, port)
func ParseRemoteAddr(remoteAddr string) (string, string) {
	parts := make([]byte, 0)
	for i := 0; i < len(remoteAddr); i++ {
		if remoteAddr[i] == ':' {
			return string(parts), remoteAddr[i+1:]
		}
		parts = append(parts, remoteAddr[i])
	}
	return string(parts), ""
}
