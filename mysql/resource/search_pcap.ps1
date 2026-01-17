# 读取 pcapng 文件并搜索 COM_STMT_EXECUTE 包
$filePath = "d:\code\db\mysql\resource\mysql.pcapng"
$data = [System.IO.File]::ReadAllBytes($filePath)

Write-Host "文件大小: $($data.Length) 字节"
Write-Host "搜索 COM_STMT_EXECUTE 包 (0x17)..."

$found = 0

# 搜索模式：[length(3 bytes)][seq(1 byte)][0x17]
for ($i = 0; $i -lt $data.Length - 10; $i++) {
    # 检查是否是可能的 MySQL 包头
    $length = $data[$i] + ($data[$i+1] -shl 8) + ($data[$i+2] -shl 16)

    if ($length -gt 0 -and $length -lt 10000 -and ($i + 4 + $length) -lt $data.Length) {
        # 检查是否是 COM_STMT_EXECUTE (0x17)
        if ($data[$i+3] -eq 0x17) {
            $found++

            Write-Host "`n=== 找到 COM_STMT_EXECUTE 包 #$found (offset: 0x$($i.ToString('x'))) ==="

            $packetData = $data[$i..($i+3+$length-1)]
            $hexStr = ($packetData | ForEach-Object { $_.ToString("x2") }) -join ''

            # 限制显示
            if ($hexStr.Length -gt 200) {
                $hexStr = $hexStr.Substring(0, 200) + "..."
            }

            Write-Host "Hex: $hexStr"
            Write-Host "长度: $length 字节"

            # 解析包
            Write-Host "  包结构:"
            Write-Host "    包头: $($packetData[0].ToString('x2')) $($packetData[1].ToString('x2')) $($packetData[2].ToString('x2')) $($packetData[3].ToString('x2'))"
            Write-Host "    Payload:"

            if ($packetData.Length -ge 13) {
                # 解析字段
                $statementID = $packetData[5] + ($packetData[6] -shl 8) + ($packetData[7] -shl 16) + ($packetData[8] -shl 24)
                $flags = $packetData[9]
                $iterationCount = $packetData[10] + ($packetData[11] -shl 8) + ($packetData[12] -shl 16) + ($packetData[13] -shl 24)

                Write-Host "      Command: 0x$($packetData[4].ToString('x2')) (COM_STMT_EXECUTE)"
                Write-Host "      StatementID: $statementID"
                Write-Host "      Flags: 0x$($flags.ToString('x2'))"
                Write-Host "      IterationCount: $iterationCount"

                # NULL bitmap 和 NewParamsBindFlag
                if ($packetData.Length -ge 15) {
                    $nullBitmap = $packetData[14]
                    $newParamsBindFlag = $packetData[15]

                    Write-Host "      NullBitmap: 0x$($nullBitmap.ToString('x2'))"
                    Write-Host "      NewParamsBindFlag: $newParamsBindFlag"

                    # 如果有参数类型
                    if ($newParamsBindFlag -eq 1 -and $packetData.Length -ge 18) {
                        Write-Host "      ParamTypes:"
                        for ($j = 0; $j -lt [Math]::Min(5, ($packetData.Length - 16) / 2); $j++) {
                            $typeIdx = 16 + $j * 2
                            $paramType = $packetData[$typeIdx]
                            $paramFlag = $packetData[$typeIdx+1]
                            Write-Host "        [$j] Type=0x$($paramType.ToString('x2')), Flag=0x$($paramFlag.ToString('x2'))"
                        }

                        # 参数值
                        $valuesStart = 16 + [Math]::Min(5, ($packetData.Length - 16) / 2) * 2
                        if ($valuesStart -lt $packetData.Length) {
                            $valueData = $packetData[$valuesStart..($packetData.Length-1)]
                            $valuesHex = ($valueData | ForEach-Object { $_.ToString("x2") }) -join ''
                            Write-Host "      ParamValues (hex): $valuesHex"
                        }
                    }
                }
            }
        }
    }
}

Write-Host "`n总共找到 $found 个 COM_STMT_EXECUTE 包"
