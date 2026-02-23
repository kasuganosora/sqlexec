package utils

import (
	"fmt"
	"testing"
)

func TestParseRemoteAddr(t *testing.T) {
	tests := []struct {
		name         string
		remoteAddr   string
		expectedIP   string
		expectedPort string
	}{
		// 正常情况
		{
			name:         "IPv4地址",
			remoteAddr:   "192.168.1.1:3306",
			expectedIP:   "192.168.1.1",
			expectedPort: "3306",
		},
		{
			name:         "本地地址",
			remoteAddr:   "127.0.0.1:8080",
			expectedIP:   "127.0.0.1",
			expectedPort: "8080",
		},
		{
			name:         "简单IP",
			remoteAddr:   "10.0.0.1:1234",
			expectedIP:   "10.0.0.1",
			expectedPort: "1234",
		},
		{
			name:         "IPv6地址",
			remoteAddr:   "[::1]:3306",
			expectedIP:   "[::1]",
			expectedPort: "3306",
		},
		{
			name:         "IPv6完整地址",
			remoteAddr:   "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
			expectedIP:   "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
			expectedPort: "8080",
		},
		{
			name:         "IPv6简写地址",
			remoteAddr:   "[2001:db8::1]:3306",
			expectedIP:   "[2001:db8::1]",
			expectedPort: "3306",
		},
		// 边界情况
		{
			name:         "空字符串",
			remoteAddr:   "",
			expectedIP:   "",
			expectedPort: "",
		},
		{
			name:         "只有IP没有端口",
			remoteAddr:   "192.168.1.1",
			expectedIP:   "192.168.1.1",
			expectedPort: "",
		},
		{
			name:         "只有冒号",
			remoteAddr:   ":",
			expectedIP:   "",
			expectedPort: "",
		},
		{
			name:         "只有端口",
			remoteAddr:   ":3306",
			expectedIP:   "",
			expectedPort: "3306",
		},
		{
			name:         "Multiple colons (no brackets)",
			remoteAddr:   "192:168:1:1:3306",
			expectedIP:   "192:168:1:1",
			expectedPort: "3306",
		},
		// 特殊情况
		{
			name:         "零端口",
			remoteAddr:   "192.168.1.1:0",
			expectedIP:   "192.168.1.1",
			expectedPort: "0",
		},
		{
			name:         "大端口",
			remoteAddr:   "192.168.1.1:65535",
			expectedIP:   "192.168.1.1",
			expectedPort: "65535",
		},
		{
			name:         "非常大的端口",
			remoteAddr:   "192.168.1.1:99999",
			expectedIP:   "192.168.1.1",
			expectedPort: "99999",
		},
		{
			name:         "单字符IP",
			remoteAddr:   "a:1",
			expectedIP:   "a",
			expectedPort: "1",
		},
		{
			name:         "特殊字符IP",
			remoteAddr:   "!@#$%^&*():123",
			expectedIP:   "!@#$%^&*()",
			expectedPort: "123",
		},
		{
			name:         "中文IP",
			remoteAddr:   "中文地址:3306",
			expectedIP:   "中文地址",
			expectedPort: "3306",
		},
		// 格式错误
		{
			name:         "只有冒号开头",
			remoteAddr:   ":8080",
			expectedIP:   "",
			expectedPort: "8080",
		},
		{
			name:         "只有冒号结尾",
			remoteAddr:   "192.168.1.1:",
			expectedIP:   "192.168.1.1",
			expectedPort: "",
		},
		// 特殊端口格式
		{
			name:         "端口带字母",
			remoteAddr:   "192.168.1.1:abc",
			expectedIP:   "192.168.1.1",
			expectedPort: "abc",
		},
		{
			name:         "端口带特殊字符",
			remoteAddr:   "192.168.1.1:!@#",
			expectedIP:   "192.168.1.1",
			expectedPort: "!@#",
		},
		// 域名
		{
			name:         "域名地址",
			remoteAddr:   "localhost:3306",
			expectedIP:   "localhost",
			expectedPort: "3306",
		},
		{
			name:         "完整域名",
			remoteAddr:   "example.com:80",
			expectedIP:   "example.com",
			expectedPort: "80",
		},
		{
			name:         "域名带路径",
			remoteAddr:   "example.com/path:8080",
			expectedIP:   "example.com/path",
			expectedPort: "8080",
		},
		// IPv6 boundary cases
		{
			name:         "IPv6 without brackets",
			remoteAddr:   "2001:db8::1:8080",
			expectedIP:   "2001:db8::1",
			expectedPort: "8080",
		},
		{
			name:         "IPv6 single opening bracket",
			remoteAddr:   "[2001:db8::1",
			expectedIP:   "[2001:db8::1",
			expectedPort: "",
		},
		{
			name:         "IPv6 single closing bracket",
			remoteAddr:   "2001:db8::1]:8080",
			expectedIP:   "2001:db8::1]",
			expectedPort: "8080",
		},
		// 长地址
		{
			name:         "长IP地址",
			remoteAddr:   "192.168.255.255.255.255.255:3306",
			expectedIP:   "192.168.255.255.255.255.255",
			expectedPort: "3306",
		},
		{
			name:         "长端口",
			remoteAddr:   "192.168.1.1:123456789",
			expectedIP:   "192.168.1.1",
			expectedPort: "123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port := ParseRemoteAddr(tt.remoteAddr)
			if ip != tt.expectedIP {
				t.Errorf("ParseRemoteAddr(%q) IP = %q, want %q", tt.remoteAddr, ip, tt.expectedIP)
			}
			if port != tt.expectedPort {
				t.Errorf("ParseRemoteAddr(%q) Port = %q, want %q", tt.remoteAddr, port, tt.expectedPort)
			}
		})
	}
}

func TestParseRemoteAddrCommonCases(t *testing.T) {
	// 常见用例测试
	tests := []struct {
		remoteAddr   string
		expectedIP   string
		expectedPort string
	}{
		{"localhost:3306", "localhost", "3306"},
		{"0.0.0.0:0", "0.0.0.0", "0"},
		{"192.168.1.1:3306", "192.168.1.1", "3306"},
		{"10.0.0.1:5432", "10.0.0.1", "5432"},
		{"[::1]:8080", "[::1]", "8080"},
		{"example.com:80", "example.com", "80"},
	}

	for _, tt := range tests {
		ip, port := ParseRemoteAddr(tt.remoteAddr)
		if ip != tt.expectedIP || port != tt.expectedPort {
			t.Errorf("ParseRemoteAddr(%q) = (%q, %q), want (%q, %q)",
				tt.remoteAddr, ip, port, tt.expectedIP, tt.expectedPort)
		}
	}
}

func TestParseRemoteAddrEdgeCases(t *testing.T) {
	// 边界情况详细测试
	tests := []struct {
		name       string
		remoteAddr string
		checkFunc  func(string, string) bool
	}{
		{
			name:       "空字符串",
			remoteAddr: "",
			checkFunc:  func(ip, port string) bool { return ip == "" && port == "" },
		},
		{
			name:       "单个字符",
			remoteAddr: "a",
			checkFunc:  func(ip, port string) bool { return ip == "a" && port == "" },
		},
		{
			name:       "单个冒号",
			remoteAddr: ":",
			checkFunc:  func(ip, port string) bool { return ip == "" && port == "" },
		},
		{
			name:       "只有冒号和端口",
			remoteAddr: ":8080",
			checkFunc:  func(ip, port string) bool { return ip == "" && port == "8080" },
		},
		{
			name:       "Multiple consecutive colons",
			remoteAddr: ":::",
			checkFunc:  func(ip, port string) bool { return ip == "::" && port == "" },
		},
		{
			name:       "Colon at start and end",
			remoteAddr: ":192.168.1.1:",
			checkFunc:  func(ip, port string) bool { return ip == ":192.168.1.1" && port == "" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port := ParseRemoteAddr(tt.remoteAddr)
			if !tt.checkFunc(ip, port) {
				t.Errorf("ParseRemoteAddr(%q) = (%q, %q) failed check", tt.remoteAddr, ip, port)
			}
		})
	}
}

func TestParseRemoteAddrIPv6(t *testing.T) {
	// IPv6 专用测试
	tests := []struct {
		name         string
		remoteAddr   string
		expectedIP   string
		expectedPort string
	}{
		{
			name:         "IPv6回环地址",
			remoteAddr:   "[::1]:3306",
			expectedIP:   "[::1]",
			expectedPort: "3306",
		},
		{
			name:         "IPv6全零",
			remoteAddr:   "[::]:8080",
			expectedIP:   "[::]",
			expectedPort: "8080",
		},
		{
			name:         "IPv6完整",
			remoteAddr:   "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:3306",
			expectedIP:   "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
			expectedPort: "3306",
		},
		{
			name:         "IPv6压缩",
			remoteAddr:   "[2001:db8::1]:8080",
			expectedIP:   "[2001:db8::1]",
			expectedPort: "8080",
		},
		{
			name:         "IPv6 without brackets",
			remoteAddr:   "2001:db8::1:8080",
			expectedIP:   "2001:db8::1",
			expectedPort: "8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port := ParseRemoteAddr(tt.remoteAddr)
			if ip != tt.expectedIP || port != tt.expectedPort {
				t.Errorf("ParseRemoteAddr(%q) = (%q, %q), want (%q, %q)",
					tt.remoteAddr, ip, port, tt.expectedIP, tt.expectedPort)
			}
		})
	}
}

func TestParseRemoteAddrInvalidInput(t *testing.T) {
	// 测试无效输入
	tests := []struct {
		name       string
		remoteAddr string
	}{
		{"控制字符", "\x00\x01\x02:3306"},
		{"制表符", "\t:3306"},
		{"换行符", "\n:3306"},
		{"回车符", "\r:3306"},
		{"多个空格", "    :3306"},
		{"Unicode空格", "\u2000:3306"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 不应该panic，应该返回某个结果
			ip, port := ParseRemoteAddr(tt.remoteAddr)
			_ = ip
			_ = port
		})
	}
}

func BenchmarkParseRemoteAddrIPv4(b *testing.B) {
	addr := "192.168.1.1:3306"
	for i := 0; i < b.N; i++ {
		ParseRemoteAddr(addr)
	}
}

func BenchmarkParseRemoteAddrIPv6(b *testing.B) {
	addr := "[2001:db8::1]:3306"
	for i := 0; i < b.N; i++ {
		ParseRemoteAddr(addr)
	}
}

func BenchmarkParseRemoteAddrLocalhost(b *testing.B) {
	addr := "localhost:3306"
	for i := 0; i < b.N; i++ {
		ParseRemoteAddr(addr)
	}
}

func ExampleParseRemoteAddr() {
	// IPv4 地址
	ip, port := ParseRemoteAddr("192.168.1.1:3306")
	fmt.Println("IP:", ip, "Port:", port)

	// 域名地址
	ip2, port2 := ParseRemoteAddr("localhost:3306")
	fmt.Println("IP:", ip2, "Port:", port2)

	// Output:
	// IP: 192.168.1.1 Port: 3306
	// IP: localhost Port: 3306
}
