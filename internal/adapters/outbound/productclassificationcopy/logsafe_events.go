package productclassificationcopy

// Event-derived values (CloudEvent ids, types, subjects, payload fields, decode
// and use-case errors) are external input: strip CR and LF before they reach a
// log attribute so a crafted event cannot forge a log line.

// sanitizeErr is sanitizeLog for an error's text ("" for a nil error).
func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeLog(err.Error())
}
