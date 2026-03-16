// contains basic helper functions

package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
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

func fmtDuration(d time.Duration) string {
	sec := d.Seconds()

	if sec >= 60 {
		return fmt.Sprintf("%.3fmin", sec/60)
	}
	if sec >= 1 {
		return fmt.Sprintf("%.3fs", sec)
	}
	if d >= time.Millisecond {
		return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
	}
	if d >= time.Microsecond {
		return fmt.Sprintf("%.3fµs", float64(d)/float64(time.Microsecond))
	}
	return fmt.Sprintf("%dns", d)
}

func toString(data interface{}) string {
	bytes, _ := json.MarshalIndent(data, "", "    ")
	return string(bytes)
}
