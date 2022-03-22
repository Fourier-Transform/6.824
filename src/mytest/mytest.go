package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
)

func main() {
	// tmpFile, err := ioutil.TempFile("./tmp", "prefix-")
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// fmt.Println("Created File: " + tmpFile.Name())
	// // fmt.Println("Created File: " + tmpFile.)
	// _, err = tmpFile.Write([]byte("This is a example!"))
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// tmpFile.Close()
	// os.Rename(tmpFile.Name(), "./tmp/renamedfile1")
	pwd, _ := os.Getwd()
	tmpPwd := pwd + "/tmp"
	fileInfoList, err := ioutil.ReadDir(tmpPwd)
	if err != nil {
		log.Fatal(err)
	}
	r, _ := regexp.Compile("prefix-([0-9]+)-23")
	fmt.Println(tmpPwd)
	for i := range fileInfoList {
		if r.MatchString(fileInfoList[i].Name()) {
			fmt.Println(fileInfoList[i].Name())
		}
	}
}
