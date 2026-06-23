package protocol

type IncomingMessage struct {
	Type    string `json:"type"`
	File    string `json:"file,omitempty"`
	Backlog int    `json:"backlog,omitempty"`
}

type OutgoingLine struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

type OutgoingBacklog struct {
	Type  string   `json:"type"`
	Lines []string `json:"lines"`
	File  string   `json:"file"`
}

type OutgoingStatus struct {
	Type     string `json:"type"`
	Watching bool   `json:"watching"`
	File     string `json:"file"`
	Position int64  `json:"position"`
}

type OutgoingHeartbeat struct {
	Type string `json:"type"`
}
