package swllib

const (
	CHUNK_ERROR = iota
	CHUNK_DATA
	CHUNK_COLLECTION_START
	CHUNK_COMMAND
	CHUNK_MESSAGE
)

type Data map[string]interface{}

type MessageChunk struct {
	Kind    string
	Message string
	Payload interface{}
}

type CollectionStartChunk struct {
	Name      string
	TypeHints map[string]string
}
