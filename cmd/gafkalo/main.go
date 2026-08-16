package main

import (
	"fmt"
	"os"

	"github.com/kmetaxas/gafkalo"
)

func main() {
	if err := gafkalo.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
