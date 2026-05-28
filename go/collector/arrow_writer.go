package main

import (
	"bytes"
	"encoding/json"
)

type ArrowDataWriter struct {
	buffer *bytes.Buffer
}

func NewArrowDataWriter() *ArrowDataWriter {
	return &ArrowDataWriter{
		buffer: new(bytes.Buffer),
	}
}

func (w *ArrowDataWriter) WriteRecord(data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	w.buffer.Write(jsonData)
	w.buffer.WriteByte('\n')
	return nil
}

func (w *ArrowDataWriter) GetBytes() []byte {
	return w.buffer.Bytes()
}

func (w *ArrowDataWriter) Size() int {
	return w.buffer.Len()
}

func (w *ArrowDataWriter) Reset() {
	w.buffer.Reset()
}
