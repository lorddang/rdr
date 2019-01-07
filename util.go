package main

import (
	"os"
	"regexp"
	"strings"
)

func isAbsPath(filePath string) bool {
	if strings.HasPrefix(filePath, "/") {
		return true
	}
	return false
}

func isExsist(filePath string) bool {
	_, err := os.Stat(filePath)
	if err == nil {
		return true
	} else {
		return false
	}
}

func isVaildPort(port string) bool {
	pattern := "[0-9]{4,5}"
	matched, _ := regexp.Match(pattern, []byte(port))
	return matched
}
