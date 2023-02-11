package lsp

import "time"

const MAXN = 1024

type MessageData struct {
	Message    *Message
	curBackoff int // 0 1 2 4 8 ...
	lastTime   time.Time
	firstTime  time.Time
	timeout    bool
}

func CreateMessageData(msg *Message) *MessageData {
	return &MessageData{
		Message: msg,
	}
}
