package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
)

type Port struct {
	Id   int    `json:"id"`
	City string `json:"city"`
}
type PortService struct {
	Ctx         context.Context
	cancel      context.CancelFunc
	m           sync.Mutex
	path        string
	buffer_path string
}

func InitPortService(p string) (*PortService, error) {
	//check file
	if file, err := os.Open(p); err != nil {
		//create

		os.Create(p)
	} else {
		file.Close()
	}
	ctx, c := context.WithCancel(context.Background())
	return &PortService{
		Ctx:         ctx,
		cancel:      c,
		m:           sync.Mutex{},
		path:        p,
		buffer_path: p + ".tmp",
	}, nil
}
func (p *PortService) Shutdown() {
	p.cancel()
}
func (p *PortService) write(ports []Port) error {
	file, err := os.Create(p.path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	for _, port := range ports {
		if err := encoder.Encode(port); err != nil {
			return err
		}
	}

	return nil
}

func (p *PortService) Submit(port Port) error {
	p.m.Lock()
	defer p.m.Unlock()
	if p.Ctx.Err() != nil {
		return errors.New("closed port service")
	}
	var rename bool

	defer func() {
		if rename {
			if err := os.Rename(p.buffer_path, p.path); err != nil {
				log.Print(err)
			}
		}
	}()
	file, err := os.Open(p.path)
	if err != nil {

		return errors.New("open file err:" + err.Error())
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var close_buffer_file bool
	var IsFound bool
	defer func() {
		if close_buffer_file {
			os.Remove(p.buffer_path)
		}
	}()
	buffer, err := os.Create(p.buffer_path)
	if err != nil {

		return errors.New("create buffer err:" + err.Error())
	}
	defer buffer.Close()
	encoder := json.NewEncoder(buffer)
	for {
		//make
		findport := Port{}
		//read
		if err := decoder.Decode(&findport); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		//check
		if findport.Id == port.Id {
			IsFound = true
			if findport == port {
				close_buffer_file = true
				return errors.New("already stored")
			}
			findport = port
		}
		//write
		if err := encoder.Encode(findport); err != nil {
			close_buffer_file = true
			return err
		}

	}
	if !IsFound {
		if err := encoder.Encode(port); err != nil {
			close_buffer_file = true
			return err
		}
	}
	//change files
	rename = true
	return nil

}
func main() {

	PortService, err := InitPortService("ports.ndjson")
	if err != nil {
		log.Fatal(err)
	}

	PortService.Submit(Port{Id: 1, City: "fds"})
}
