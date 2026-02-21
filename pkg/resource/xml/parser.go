package xml

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"

	"golang.org/x/text/encoding/unicode"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// xmlNode 通用 XML 节点，用于解析任意结构的 XML
type xmlNode struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Children []xmlNode  `xml:",any"`
	Content  string     `xml:",chardata"`
}

// decodeXMLBytes 将可能是 UTF-16 编码的 XML 字节解码为 UTF-8
func decodeXMLBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	// 检测 BOM 判断是否为 UTF-16
	isUTF16 := false
	if len(data) >= 2 {
		// UTF-16 LE BOM: FF FE
		if data[0] == 0xFF && data[1] == 0xFE {
			isUTF16 = true
		}
		// UTF-16 BE BOM: FE FF
		if data[0] == 0xFE && data[1] == 0xFF {
			isUTF16 = true
		}
	}

	// 没有 BOM 但可能仍然是 UTF-16（检查第二个字节是否为 0）
	if !isUTF16 && len(data) >= 4 {
		if data[1] == 0 || data[0] == 0 {
			isUTF16 = true
		}
	}

	if !isUTF16 {
		return data, nil
	}

	// 使用 golang.org/x/text 解码 UTF-16
	decoder := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()
	utf8Bytes, err := decoder.Bytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode UTF-16: %w", err)
	}

	// 替换 encoding="utf-16" 为 encoding="utf-8"，因为数据现在是 UTF-8
	utf8Bytes = bytes.Replace(utf8Bytes, []byte(`encoding="utf-16"`), []byte(`encoding="utf-8"`), 1)

	return utf8Bytes, nil
}

// encodeToUTF16 将 UTF-8 字节编码为 UTF-16 LE（带 BOM）
func encodeToUTF16(data []byte) ([]byte, error) {
	encoder := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewEncoder()
	return encoder.Bytes(data)
}

// parseXMLFile 解析单个 XML 文件，返回行数据、根标签名、属性列名列表
// 支持三种模式：
//  1. 属性模式：根元素的属性作为列
//  2. 简单子元素模式：只含文本的子元素作为列
//  3. 列表展开模式：容器中重复子元素展开为多行
func parseXMLFile(data []byte, filename string, expandLists bool) ([]domain.Row, string, []string, error) {
	// 解码 UTF-16
	utf8Data, err := decodeXMLBytes(data)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to decode XML file %s: %w", filename, err)
	}

	// 解析 XML
	var root xmlNode
	if err := xml.Unmarshal(utf8Data, &root); err != nil {
		return nil, "", nil, fmt.Errorf("failed to parse XML file %s: %w", filename, err)
	}

	rootTag := root.XMLName.Local

	// 检查是否是列表容器模式（如 bestcook.xml）
	if expandLists && isListContainer(&root) {
		rows, attrCols := expandListContainer(&root, filename)
		return rows, rootTag, attrCols, nil
	}

	// 标准模式：一个文件一行
	row := domain.Row{
		"_file": filename,
	}

	// 提取根元素属性
	attrCols := make([]string, 0, len(root.Attrs))
	for _, attr := range root.Attrs {
		colName := attr.Name.Local
		row[colName] = attr.Value
		attrCols = append(attrCols, colName)
	}

	// 处理子元素
	childGroups := groupChildren(root.Children)
	for tag, children := range childGroups {
		if len(children) == 1 && isSimpleTextNode(&children[0]) {
			// 简单文本子元素：直接作为列
			row[tag] = strings.TrimSpace(children[0].Content)
		} else {
			// 复杂子元素或重复子元素：序列化为 JSON
			jsonVal := serializeChildrenToJSON(children)
			row[tag] = jsonVal
		}
	}

	return []domain.Row{row}, rootTag, attrCols, nil
}

// isListContainer 检查是否是列表容器模式
// 条件：根元素无属性（或很少），有一个包装子元素，包装子元素内有多个同名子元素
func isListContainer(node *xmlNode) bool {
	if len(node.Children) == 0 {
		return false
	}

	// 根元素有属性的通常不是列表容器
	if len(node.Attrs) > 0 {
		return false
	}

	// 检查直接子元素
	// 可能有一个包装层，也可能直接就是列表
	if len(node.Children) == 1 {
		wrapper := &node.Children[0]
		if len(wrapper.Children) >= 2 {
			// 检查包装层内的子元素是否同名
			firstName := wrapper.Children[0].XMLName.Local
			allSame := true
			for _, child := range wrapper.Children[1:] {
				if child.XMLName.Local != firstName {
					allSame = false
					break
				}
			}
			if allSame {
				return true
			}
		}
	}

	// 直接子元素是否同名
	if len(node.Children) >= 2 {
		firstName := node.Children[0].XMLName.Local
		allSame := true
		for _, child := range node.Children[1:] {
			if child.XMLName.Local != firstName {
				allSame = false
				break
			}
		}
		if allSame {
			return true
		}
	}

	return false
}

// expandListContainer 展开列表容器为多行
func expandListContainer(root *xmlNode, filename string) ([]domain.Row, []string) {
	var items []xmlNode

	// 找到列表项
	if len(root.Children) == 1 && len(root.Children[0].Children) >= 2 {
		items = root.Children[0].Children
	} else if len(root.Children) >= 2 {
		items = root.Children
	}

	rows := make([]domain.Row, 0, len(items))
	var attrCols []string

	for i, item := range items {
		row := domain.Row{
			"_file":  filename,
			"_index": int64(i),
		}

		// 提取属性
		itemAttrCols := make([]string, 0, len(item.Attrs))
		for _, attr := range item.Attrs {
			colName := attr.Name.Local
			row[colName] = attr.Value
			itemAttrCols = append(itemAttrCols, colName)
		}
		if i == 0 {
			attrCols = itemAttrCols
		}

		// 提取简单文本子元素
		for _, child := range item.Children {
			if isSimpleTextNode(&child) {
				row[child.XMLName.Local] = strings.TrimSpace(child.Content)
			} else {
				// 复杂子元素序列化为 JSON
				row[child.XMLName.Local] = serializeNodeToJSON(&child)
			}
		}

		rows = append(rows, row)
	}

	return rows, attrCols
}

// isSimpleTextNode 检查节点是否只包含文本内容（无属性、无子元素）
func isSimpleTextNode(node *xmlNode) bool {
	return len(node.Attrs) == 0 && len(node.Children) == 0 && strings.TrimSpace(node.Content) != ""
}

// groupChildren 将子元素按标签名分组
func groupChildren(children []xmlNode) map[string][]xmlNode {
	groups := make(map[string][]xmlNode)
	for _, child := range children {
		tag := child.XMLName.Local
		groups[tag] = append(groups[tag], child)
	}
	return groups
}

// serializeChildrenToJSON 将一组子元素序列化为 JSON 字符串
func serializeChildrenToJSON(children []xmlNode) string {
	if len(children) == 1 {
		return serializeNodeToJSON(&children[0])
	}

	// 多个子元素：序列化为 JSON 数组
	items := make([]interface{}, 0, len(children))
	for _, child := range children {
		items = append(items, nodeToMap(&child))
	}
	data, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(data)
}

// serializeNodeToJSON 将单个节点序列化为 JSON 字符串
func serializeNodeToJSON(node *xmlNode) string {
	m := nodeToMap(node)
	data, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// nodeToMap 将 XML 节点转换为 map
func nodeToMap(node *xmlNode) map[string]interface{} {
	m := make(map[string]interface{})

	// 添加属性
	for _, attr := range node.Attrs {
		m[attr.Name.Local] = attr.Value
	}

	// 处理子元素
	childGroups := groupChildren(node.Children)
	for tag, children := range childGroups {
		if len(children) == 1 {
			child := &children[0]
			if isSimpleTextNode(child) {
				m[tag] = strings.TrimSpace(child.Content)
			} else {
				m[tag] = nodeToMap(child)
			}
		} else {
			items := make([]interface{}, 0, len(children))
			for _, child := range children {
				if isSimpleTextNode(&child) {
					items = append(items, strings.TrimSpace(child.Content))
				} else {
					items = append(items, nodeToMap(&child))
				}
			}
			m[tag] = items
		}
	}

	// 文本内容
	text := strings.TrimSpace(node.Content)
	if text != "" && len(node.Children) == 0 {
		m["_text"] = text
	}

	return m
}
