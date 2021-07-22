package mq

import (
	"encoding/json"
	"time"

	"github.com/stewelarend/logger"
)

//IMessage is sent through mq system and is immutable, hence the GetXxx method names
type IMessage interface {
	GetTimestamp() time.Time
	GetData() []byte
}

//message implements IMessage and can be serialised as JSON
//so it must have public scope field names
type message struct {
	Timestamp time.Time
	Data      []byte
}

//NewMessage creates an IMessage
func NewMessage(data []byte) IMessage {
	return message{
		Timestamp: time.Now(),
		Data:      data,
	}
}

//DecodeMessage decodes JSON into message struct
func DecodeMessage(serializedData []byte) (IMessage, error) {
	var msg message
	err := json.Unmarshal(serializedData, &msg)
	if err != nil {
		return message{}, logger.Wrapf(err, "Failed to decode IMessage")
	}
	return msg, nil
}

func (m message) GetTimestamp() time.Time {
	return m.Timestamp
}

func (m message) GetData() []byte {
	return m.Data
}
