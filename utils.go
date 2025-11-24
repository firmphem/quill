// contains basic helper functions

package main

import (
	"encoding/json"
	"reflect"
)

func toFloat64(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int, int32, int64:
		return float64(reflect.ValueOf(t).Int()), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
