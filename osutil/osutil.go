package osutil

import (
	"os"
)

func IsExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}
