package io

// SplitPacket 将大包拆分成多个小包
func (i *IO) SplitPacket(packetData []byte) ([][]byte, error) {
	if len(packetData) <= int(i.maxPacketSize) {
		return [][]byte{packetData}, nil
	}

	var packets [][]byte
	offset := 0
	sequenceID := uint8(0)

	for offset < len(packetData) {
		// 计算当前包的大小
		remaining := len(packetData) - offset
		packetSize := remaining
		if packetSize > int(i.maxPacketSize) {
			packetSize = int(i.maxPacketSize)
		}

		// 创建包头
		header := make([]byte, 4)
		header[0] = byte(packetSize)
		header[1] = byte(packetSize >> 8)
		header[2] = byte(packetSize >> 16)
		header[3] = sequenceID

		// 创建包数据
		packet := make([]byte, 4+packetSize)
		copy(packet[:4], header)
		copy(packet[4:], packetData[offset:offset+packetSize])

		packets = append(packets, packet)

		offset += packetSize
		sequenceID++
	}

	return packets, nil
}

// AssemblePacket 组装多个包成一个完整的包
func (i *IO) AssemblePacket(packets [][]byte) ([]byte, error) {
	if len(packets) == 0 {
		return nil, ErrInvalidPacket
	}

	if len(packets) == 1 {
		return packets[0], nil
	}

	// 计算总大小
	totalSize := 0
	for _, packet := range packets {
		if len(packet) < 4 {
			return nil, ErrInvalidPacket
		}
		payloadLength := uint32(packet[0]) | uint32(packet[1])<<8 | uint32(packet[2])<<16
		totalSize += int(payloadLength)
	}

	// 组装包
	assembled := make([]byte, totalSize)
	offset := 0

	for _, packet := range packets {
		payloadLength := uint32(packet[0]) | uint32(packet[1])<<8 | uint32(packet[2])<<16
		payload := packet[4 : 4+payloadLength]

		copy(assembled[offset:], payload)
		offset += int(payloadLength)
	}

	return assembled, nil
}

// WriteLargePacket 写入大包，自动拆分
func (i *IO) WriteLargePacket(packetData []byte) error {
	// 拆分包
	packets, err := i.SplitPacket(packetData)
	if err != nil {
		return err
	}

	// 写入所有包
	for _, packet := range packets {
		if err := i.WritePacket(packet); err != nil {
			return err
		}
	}

	return nil
}

// ReadLargePacket 读取大包，自动组装
func (i *IO) ReadLargePacket() ([]byte, error) {
	var packets [][]byte
	expectedSequenceID := uint8(0)

	for {
		// 读取单个包
		packet, err := i.readPacket()
		if err != nil {
			return nil, err
		}

		if len(packet) < 4 {
			return nil, ErrInvalidPacket
		}

		// 解析包头
		payloadLength := uint32(packet[0]) | uint32(packet[1])<<8 | uint32(packet[2])<<16
		sequenceID := packet[3]

		// 检查序号
		if sequenceID != expectedSequenceID {
			return nil, ErrInvalidPacket
		}

		packets = append(packets, packet)
		expectedSequenceID++

		// 如果当前包小于最大包大小，说明是最后一个包
		if payloadLength < i.maxPacketSize {
			break
		}
	}

	// 组装包
	return i.AssemblePacket(packets)
}
