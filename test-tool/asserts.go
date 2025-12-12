package main

import (
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------
func bothArgsAreStrings(got, expected any) (string, string, error) {
	if gs, ok := got.(string); ok {
		es, ok2 := expected.(string)
		if !ok2 {
			return "", "", fmt.Errorf("expected (as expected value) string but got %T", expected)
		}
		return gs, es, nil
	}
	return "", "", fmt.Errorf("expected (as return value) string but got %T", got)
}

// -----------------------------------------------------------------------------
func bothArgsAreInts(got, expected any) (int, int, error) {
	if gi, ok := toInt(got); ok {
		ei, ok2 := toInt(expected)
		if !ok2 {
			return 0, 0, fmt.Errorf("expected (as expected value) string but got %T", expected)
		}
		return gi, ei, nil
	}
	return 0, 0, fmt.Errorf("expected (as return value) string but got %T", got)
}

// -----------------------------------------------------------------------------
func (as Assert) isEmpty() bool {
	return as.Condition == ""
}

// -----------------------------------------------------------------------------
func (as Assert) assertResultToString(err error) string {
	if err != nil {
		return fmt.Sprintf("%s%s '%v' failed due to error: %v%s", ColorRed, as.Condition, as.Expected, err.Error(), ColorReset)
	}

	return fmt.Sprintf("%s%s '%v'%s", ColorGreen, as.Condition, as.Expected, ColorReset)
}

// -----------------------------------------------------------------------------
func (as *Assert) evaluateAssert(got any) error {
	expected := as.Expected

	if got == nil && expected == nil {
		return nil
	}

	switch as.Condition {
	case "contains":
		gs, es, err := bothArgsAreStrings(got, expected)
		if err != nil {
			return err
		}
		if !strings.Contains(gs, es) {
			return fmt.Errorf("got string does not contain expected string: got='%v' expected='%v'", gs, es)
		}
		return nil

	case "=":
		// ints
		gi, ei, err := bothArgsAreInts(got, expected)
		if err == nil {
			if gi != ei {
				return fmt.Errorf("string mismatch: got='%v' expected='%v'", gi, ei)
			}
			return nil
		}

		// strings
		gs, es, err := bothArgsAreStrings(got, expected)
		if err == nil {
			if gs != es {
				return fmt.Errorf("string mismatch: got='%v' expected='%v'", gs, es)
			}
			return nil
		}

		// rest things
		return fmt.Errorf("unsupported types to compare: got=%T expected=%T (script handles string, int)", got, expected)

	default:
		return fmt.Errorf("unknown assert condition: %s", as.Condition)
	}

}
