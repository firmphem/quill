package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// -----------------------------------------------------------------------------
func fnBashCmdExecutor(cmdLine string, ignoreErrors bool, printOutput bool) (any, error) {
	args := []string{"-c", strings.Replace(cmdLine, "%CONFIG_FILE%", "--config "+configFile, -1)}
	cmd := exec.Command("/bin/bash", args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	stdoutStr := strings.TrimSpace(stdout.String())
	// stderrStr := strings.TrimSpace(stderr.String())

	if stdoutStr != "" && printOutput {
		// log.Info().Str("stdout", stdoutStr).Msg("shell: stdout")
		os.Stdout.Write(stdout.Bytes())
	}
	// if stderrStr != "" {
	// 	log.Warn().Str("stderr", stderrStr).Msg("shell: stderr")
	// }

	if err != nil && !ignoreErrors {
		return stdout.Bytes(), fmt.Errorf("shell command failed. See the output: %w", err)
	}

	return stdout.Bytes(), nil
}
