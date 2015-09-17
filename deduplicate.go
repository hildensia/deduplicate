package main

import (
  "sync"
  "flag"
  "os"
  "io"
  "bufio"
  "path/filepath"
  "crypto/md5"
  "encoding/hex"
)

type File struct {
  hash string
  path string
}

func readBytes(filename string, bytes int) []byte {
  // open input file
  fi, err := os.Open(filename)
  if err != nil {
    panic(err)
  }
  // close fi on exit and check for its returned error
  defer func() {
    if err := fi.Close(); err != nil {
      panic(err)
    }
  }()

  r := bufio.NewReader(fi)

  buf := make([]byte, bytes)
  // read a chunk
  _, err = r.Read(buf)
  if err != nil && err != io.EOF {
    panic(err)
  }

  return buf
}

func HashFile(ch chan<- File, wg *sync.WaitGroup) func(string, os.FileInfo, error) error {
  // do to limits on open files we simply use a channel to block
  file_c := make(chan int, 128)
  return func(path string, info os.FileInfo, err error) error {
    if info.IsDir() {
      return nil
    }
    wg.Add(1)
    go func() {
      file_c <- 1
      chunk := md5.Sum(readBytes(path, 4096))
      <- file_c
      hash := hex.EncodeToString(chunk[:])
      ch <- File{hash: hash, path: path}
    }()

    return nil
  }
}

func CheckDuplicates(ch <-chan File, wg *sync.WaitGroup) {
  files := make(map[string]string)
  for {
    f := <- ch
    original, duplicate := files[f.hash]
    if duplicate {
      println(f.path + " == " + original)  
    } else {
      files[f.hash] = f.path  
    }
    wg.Done()
  }
}

func main() {
  ch := make(chan File)
  var wg sync.WaitGroup
  var dir string
  flag.StringVar(&dir, "dir", ".", "Which directory to parse for duplicates")
  flag.Parse()
  filepath.Walk(dir, HashFile(ch, &wg))  
  go CheckDuplicates(ch, &wg)
  wg.Wait()
}
