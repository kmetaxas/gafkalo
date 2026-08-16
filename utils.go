package gafkalo

func SafeNullStr(str *string) string {
	defaultVal := "NIL"
	if str == nil {
		return defaultVal
	}
	return *str
}
