package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ColorReset = "\033[0m"
	ColorRed   = "\033[31m"
	ColorGreen = "\033[32m"

	ColorGray      = "\033[90m"
	ColorLightGray = "\033[97m"
)

// -----------------------------------------------------------------------------
func join(list []string, sep string) string {
	if len(list) == 0 {
		return ""
	}
	out := list[0]
	for _, v := range list[1:] {
		out += sep + v
	}
	return out
}

// -----------------------------------------------------------------------------
func isYAML(name string) bool {
	return strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml")
}

// -----------------------------------------------------------------------------
func toString(v interface{}) string {
	jsonData, _ := json.Marshal(v)
	return string(jsonData)
}

// -----------------------------------------------------------------------------
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	// case float64:
	// 	return int(n), true
	default:
		return 0, false
	}
}

// -----------------------------------------------------------------------------
func paintInColor(color, text string) string {
	return fmt.Sprintf("%s%s%s", color, text, ColorReset)
}
