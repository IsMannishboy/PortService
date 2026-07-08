package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

//ERRORS

var AlreadyStoredPort = errors.New("lready stored")

// /
type BinPort struct {
	Id   [16]byte
	City [100]byte
}
type Port struct {
	Id   int
	City string
}
type ChanPort struct {
	Port Port
	Ch   chan RespCh
}
type RespCh struct {
	Err error
	Msg string
}

var SIZE = int64(unsafe.Sizeof(BinPort{}))

type PortService struct {
	ctx        context.Context
	cancel     context.CancelFunc
	recv       chan ChanPort
	size       int
	batch_size int
	file_name  string
}

var PortServiceClosed = errors.New("PortServiceClosed")

func (p *PortService) Submit(port Port) (error, chan RespCh) {
	if p.ctx.Err() != nil {
		return PortServiceClosed, nil
	}
	ch_port := ChanPort{Port: port, Ch: make(chan RespCh, 1)}
	select {
	case <-p.ctx.Done():
		return PortServiceClosed, nil
	case p.recv <- ch_port:
		return nil, ch_port.Ch
	}
}
func InitPortService(size int, byte_size int, wg *sync.WaitGroup, file_name string) (*PortService, error) {
	file, err := os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	file.Close()

	ctx, c := context.WithCancel(context.Background())
	p := &PortService{
		ctx:        ctx,
		cancel:     c,
		recv:       make(chan ChanPort),
		size:       size,
		batch_size: byte_size,
		file_name:  file_name,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run()

	}()
	return p, nil
}
func readChunk(file *os.File, batch_size int, size int) (int, []byte, error) {
	//read buffer
	buff := make([]byte, batch_size*size)
	n, err := io.ReadFull(file, buff)
	if err != nil {
		if err == io.ErrUnexpectedEOF {

			buff = buff[:n]

		} else {
			return n, nil, err
		}

	}
	return n, buff, nil
}
func rewriteChunk(file *os.File, bs int, ports []Port, n int) error {
	//bin
	bin_ports := make([]BinPort, 0, bs)
	for i := 0; i < len(ports); i++ {
		bin_ports = append(bin_ports, MakePort(ports[i]))
	}
	_, err := file.Seek(int64(-n), io.SeekCurrent)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, bin_ports)
	if err != nil {
		log.Fatal("write buffer err: ", err)
		return err
	}
	return nil

}
func (p *PortService) Run() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case req := <-p.recv:
			port := req.Port
			resp := req.Ch
			//read
			file, err := os.OpenFile(p.file_name, os.O_RDWR, 0644)
			if err != nil {
				log.Fatal("file open err:", err)

			}
			resp_str := RespCh{}

			offset := 0
			for {
				n, buff, read_err := readChunk(file, p.batch_size, p.size)
				if read_err != nil {
					if read_err == io.EOF {
						bin := MakePort(port)
						err = binary.Write(file, binary.LittleEndian, bin)
						if err != nil {
							log.Fatal(err)
							resp_str.Err = err
							resp <- RespCh{Err: err}
						}
						resp <- RespCh{Err: nil}
						break

					}
				}
				log.Print("readed ", n, " bytes")
				//to json
				ports := BufferToJSON(buff)
				for i := 0; i < len(ports); i++ {
					if ports[i].Id == port.Id {
						if ports[i] == port {
							//already stored
							resp_str = RespCh{Err: AlreadyStoredPort}
							break
						}
						ports[i] = port
						resp_str = RespCh{Err: nil, Msg: "Changed"}
						break
					}
				}
				if resp_str.Err != nil {
					resp <- resp_str
					break
				} else if resp_str.Msg == "Changed" {
					//bin
					err := rewriteChunk(file, p.batch_size, ports, n)
					if err != nil {
						log.Print("rewriteChunk err :", err)
						resp_str.Err = err
						resp_str.Msg = "write err"
					}
					resp <- resp_str

					break
				}

				offset += len(buff) * p.size

			}

			file.Close()
			close(req.Ch)

		}
	}

}
func (p *PortService) Shutdown() {

	p.cancel()
}
func MakePort(portt Port) BinPort {
	port := BinPort{}
	str := strconv.Itoa(portt.Id)
	id_limit := len(str)
	if id_limit > len(port.Id) {
		id_limit = len(port.Id)
	}
	for i := 0; i < id_limit; i++ {
		port.Id[i] = str[i]
	}

	citylimit := len(portt.City)
	if citylimit > len(port.City) {
		citylimit = len(port.City)
	}
	for i := 0; i < citylimit; i++ {
		port.City[i] = portt.City[i]
	}
	return port

}
func bytesToString(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}
func BinToJson(bin BinPort) Port {
	idStr := bytesToString(bin.Id[:])
	city := bytesToString(bin.City[:])

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Fatal(err)
	}

	return Port{
		Id:   id,
		City: city,
	}
}
func BufferToJSON(buff []byte) []Port {
	var ports []Port
	for i := 0; i < len(buff); i += int(SIZE) {
		chunk := buff[i : i+int(SIZE)]

		var p BinPort
		copy(p.Id[:], chunk[0:16])
		copy(p.City[:], chunk[16:SIZE])

		ports = append(ports, BinToJson(p))
	}
	return ports
}

func main() {
	//INIT
	wg := sync.WaitGroup{}
	s, err := InitPortService(int(unsafe.Sizeof(BinPort{})), 100, &wg, "file.bin")
	if err != nil {
		log.Fatal(err)
	}
	//test
	test_wg := sync.WaitGroup{}
	test_wg.Add(10)
	start := time.Now()
	for i := 0; i < 10; i++ {
		go func() {
			defer test_wg.Done()
			err, _ := s.Submit(Port{Id: i, City: "Boston"})
			if err != nil {
				log.Fatal("submit err:", err)
			}
		}()

	}
	test_wg.Wait()

	diff := time.Since(start)
	s.Shutdown()
	//check
	f, err := os.Open("file.bin")
	if err != nil {
		log.Fatal(err)
	}
	b := make([]byte, SIZE*100)

	n, err := io.ReadFull(f, b)
	if err != nil {
		log.Print(err)
		if err == io.ErrUnexpectedEOF {
			b = b[:n]
		} else {
			log.Fatal(err)

		}
	}
	log.Print("readed :", n)
	ports := BufferToJSON(b)
	log.Print(ports)
	wg.Wait()
	log.Print(time.Duration(diff) * time.Microsecond)

}
