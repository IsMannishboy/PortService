package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Port struct {
	Id   int    `json:"id"`
	City string `json:"city"`
}
type PortCh struct {
	Port Port
	Recv chan struct{}
}
type PortService struct {
	Ctx         context.Context
	cancel      context.CancelFunc
	recv        chan PortCh
	m           sync.Mutex
	path        string
	buffer_path string
	is_working  chan struct{}
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
	s := &PortService{
		Ctx:         ctx,
		cancel:      c,
		m:           sync.Mutex{},
		path:        p,
		buffer_path: p + ".tmp",
		recv:        make(chan PortCh, 1000),
		is_working:  make(chan struct{}),
	}

	go s.startWork()
	return s, nil
}
func (p *PortService) Shutdown() {
	p.cancel()
	<-p.is_working
	log.Print("closing port service")
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
func (p *PortService) Submit(port Port) (<-chan struct{}, error) {
	send := PortCh{
		Port: port,
		Recv: make(chan struct{}),
	}
	select {
	case p.recv <- send:
		return send.Recv, nil
	case <-p.Ctx.Done():
		return nil, errors.New("closed port service")
	}
}
func (p *PortService) startWork() {
	defer close(p.is_working)
	for {
		//make notice chan

		if p.Ctx.Err() != nil {
			log.Print("context closed before start of work")
			return
		}
		select {
		case <-p.Ctx.Done():
			return
		case req := <-p.recv:
			file, err := os.Open(p.path)
			if err != nil {
				log.Print(err)
				return
			}
			decoder := json.NewDecoder(file)
			buffer, err := os.Create(p.buffer_path)
			if err != nil {
				log.Print(err)
				return
			}
			encoder := json.NewEncoder(buffer)
			close_buffer := false
			for {
				port := Port{}
				if err := decoder.Decode(&port); err == io.EOF {
					log.Print("end of file")
					if err := encoder.Encode(req.Port); err != nil {
						close_buffer = true

						log.Print(err)
						break
					}
					break
				} else if err != nil {
					close_buffer = true

					log.Print(err)
				}
				if req.Port.Id == port.Id {
					log.Print("port found")
					if req.Port == port {
						log.Print("this value already stored")
						//this value already stored
						close_buffer = true
						break
					}
					port = req.Port
				}
				if err := encoder.Encode(port); err != nil {
					close_buffer = true

					log.Print(err)
					break
				}

			}
			//buffer file
			file.Close()
			buffer.Close()

			if close_buffer {
				log.Print("deleting buffer file")
				os.Remove(p.buffer_path)
			} else {
				os.Rename(p.buffer_path, p.path)

			}
			log.Print("new data stored")
			close(req.Recv)
		}

	}

}
func main() {
	PortService, err := InitPortService("ports.ndjson")
	if err != nil {
		log.Fatal(err)
	}
	chans := make([]<-chan struct{}, 0, 10)
	wg := sync.WaitGroup{}
	wg.Add(10)
	start := time.Now()
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			recv, err := PortService.Submit(Port{Id: i, City: "boston"})
			if err != nil {
				log.Print(err)
				return
			}
			chans = append(chans, recv)
		}(i)

	}
	duration := time.Since(start)
	wg.Wait()
	for _, k := range chans {
		<-k
	}
	log.Print(duration.Microseconds())

}
