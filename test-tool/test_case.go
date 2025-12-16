package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const identation = "   "

// -----------------------------------------------------------------------------
func loadTestSuites(root string) ([]testCase, error) {
	var cases []testCase

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk dir %s: %w", root, err)
		}

		if d.IsDir() {
			return nil
		}

		if !isYAML(path) {
			return nil
		}

		if includeTestsRegex != "" {
			if match, _ := regexp.MatchString(includeTestsRegex, path); !match {
				log.Info().Str("test", path).Str("filter", includeTestsRegex).Str("test file", path).Msg("test file was filtered out")
				return nil
			}
		}

		fileCases, err := loadOneFile(path)
		if err != nil {
			return fmt.Errorf("failed parsing %s: %w", path, err)
		}

		for i := range fileCases {
			fileCases[i].FileName = path
		}

		cases = append(cases, fileCases...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(cases) == 0 {
		return nil, errors.New("no test cases found")
	}

	return cases, nil
}

// -----------------------------------------------------------------------------
func loadOneFile(path string) ([]testCase, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// expect yaml with top-level "tests:" ---> list of testCase
	var wrapper struct {
		Tests []testCase `yaml:"tests"`
	}

	if err := yaml.Unmarshal(raw, &wrapper); err != nil {
		return nil, err
	}

	return wrapper.Tests, nil
}

// -----------------------------------------------------------------------------
func identLevel(level int) string {
	if level <= 0 {
		return ""
	}
	ident := ""
	for i := 0; i < level; i++ {
		ident = ident + identation
	}
	return ident
}

// -----------------------------------------------------------------------------
func runTestCase(tc testCase) error {
	// log.Info().Msgf("test: %s (file: %s)", tc.Name, tc.FileName)
	fmt.Printf("test: %s (file: %s)\n", tc.Name, tc.FileName)

	for i, step := range tc.Steps {
		if step.Type == "golang" {
			if len(step.Pos) > 0 {
				fmt.Printf("%s%s\n%s%s: %s(%v)\n", identLevel(1), paintInColor(ColorGray, step.Name), identLevel(2), step.Type, step.Func, step.Pos)
			} else {
				fmt.Printf("%s%s\n%s%s: %s\n", identLevel(1), paintInColor(ColorGray, step.Name), identLevel(2), step.Type, step.Func)
			}
		} else {
			fmt.Printf("%s%s\n%s%s: %s\n", identLevel(1), paintInColor(ColorGray, step.Name), identLevel(2), step.Type, step.Func)
		}

		var out any
		var callErr error

		switch step.Type {
		case "golang":
			fn, exists := registry[step.Func]
			if !exists {
				return fmt.Errorf("unknown golang function: %s", step.Func)
			}

			args, err := buildArgs(fn, step)
			if err != nil {
				return fmt.Errorf("arg error in step %d: %w", i+1, err)
			}

			results := fn.Call(args)
			if len(results) == 2 {
				if v := results[0].Interface(); v != nil {
					out = v
				}
				if e := results[1].Interface(); e != nil {
					callErr = e.(error)
				}
			}

		case "shell":
			out, callErr = fnBashCmdExecutor(step.Func, step.IgnoreErrors, step.PrintOutput)

		case "sql":
			out, callErr = fnSqlExecutor(step.Func)

		default:
			return fmt.Errorf("unknown step type '%s' in step %d", step.Type, i+1)
		}

		if callErr != nil {
			return fmt.Errorf("step %d returned error: %w", i+1, callErr)
		}

		if step.Assert.isEmpty() {
			continue
		}

		err := step.Assert.evaluateAssert(out)
		fmt.Printf("%s%s\n", identLevel(2), step.Assert.assertResultToString(err))
	}

	log.Info().Msgf("completed test '%s'", tc.Name)
	return nil
}

// -----------------------------------------------------------------------------
func buildArgs(fn reflect.Value, step testStep) ([]reflect.Value, error) {
	ft := fn.Type()

	// positional args if provided
	if len(step.Pos) > 0 {
		if len(step.Pos) != ft.NumIn() {
			return nil, fmt.Errorf("positional args count mismatch: expected %d got %d", ft.NumIn(), len(step.Pos))
		}
		return convertByPosition(ft, step.Pos)
	}

	// named args using arg1,arg2... (do we need to support them at all??)
	if len(step.Args) > 0 {
		return convertByName(ft, step.Args)
	}

	// no args
	if ft.NumIn() == 0 {
		return []reflect.Value{}, nil
	}

	return nil, errors.New("no args provided but function requires arguments")
}

// -----------------------------------------------------------------------------
func convertByPosition(ft reflect.Type, pos []interface{}) ([]reflect.Value, error) {
	args := make([]reflect.Value, len(pos))

	for i := range pos {
		wantType := ft.In(i)
		val, err := convertValue(pos[i], wantType)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	return args, nil
}

// -----------------------------------------------------------------------------
func convertByName(ft reflect.Type, named map[string]interface{}) ([]reflect.Value, error) {
	args := make([]reflect.Value, ft.NumIn())

	for i := 0; i < ft.NumIn(); i++ {
		key := fmt.Sprintf("arg%d", i+1)
		raw, exists := named[key]
		if !exists {
			return nil, fmt.Errorf("missing named arg: %s", key)
		}

		val, err := convertValue(raw, ft.In(i))
		if err != nil {
			return nil, err
		}

		args[i] = val
	}

	return args, nil
}

// -----------------------------------------------------------------------------
func convertValue(v interface{}, t reflect.Type) (reflect.Value, error) {
	switch t.Kind() {
	case reflect.String:
		s, ok := v.(string)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected string got %T", v)
		}
		return reflect.ValueOf(s), nil

	case reflect.Int:
		switch n := v.(type) {
		case int:
			return reflect.ValueOf(n), nil
		case int64:
			return reflect.ValueOf(int(n)), nil
		case int32:
			return reflect.ValueOf(int(n)), nil
		default:
			return reflect.Value{}, fmt.Errorf("expected int got %T", v)
		}

	case reflect.Int32:
		switch n := v.(type) {
		case int:
			return reflect.ValueOf(n), nil
		case int64:
			return reflect.ValueOf(int(n)), nil
		case int32:
			return reflect.ValueOf(int(n)), nil
		default:
			return reflect.Value{}, fmt.Errorf("expected int got %T", v)
		}

	case reflect.Bool:
		s, ok := v.(bool)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected bool got %T", v)
		}
		return reflect.ValueOf(s), nil

	default:
		return reflect.Value{}, fmt.Errorf("unsupported arg type: %s", t.Kind())
	}
}
