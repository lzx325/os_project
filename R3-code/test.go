package main

import (
	"fmt"
)

func return2() (int, error) {
	return 1, nil
}

func main() {
	m := map[string]int{}
	a, err := m["a"]
	fmt.Println(a, err)
}
