package main

import "fmt"

func main() {
	slice := make([]int, 10)
	for i := 0 ;i < 10;i++ {
		slice[i] = i
		fmt.Println(slice[i])
	}

}
