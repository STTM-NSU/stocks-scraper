package main

import (
	"fmt"
	"os"
)

func main() {
	for {
		s := os.Getenv("PROXIES")
		if s != "" {
			fmt.Println(s)
		} else {
			return
		}
	}
}
