package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/tarm/serial"
)

// Struktur untuk menyimpan daftar klien SSE
type SSEClient struct {
	ctx  context.Context
	data chan string
}

var (
	clients = struct {
		sync.Mutex
		list []*SSEClient
	}{}
	port       *serial.Port
	portMutex  sync.Mutex
	lastWeight string // Simpan berat terakhir yang diterima
)

// Fungsi untuk membaca data dari timbangan (RS232)
func readWeight() {
	buffer := make([]byte, 128)
	var prevData string

	for {
		portMutex.Lock()
		if port == nil {
			portMutex.Unlock()
			return
		}
		n, err := port.Read(buffer)
		portMutex.Unlock()

		if err != nil {
			log.Printf("Gagal membaca data: %v", err)
			continue
		}

		if n > 0 {
			rawData := string(buffer[:n])
			weight := parseWeight(rawData)

			if weight != "" && weight != prevData {
				fmt.Printf("Filtered Weight: %s\n", weight)
				broadcast(weight)
				prevData = weight
			}
		}
	}
}

// Fungsi untuk mengekstrak berat dari data mentah
func parseWeight(data string) string {
	re := regexp.MustCompile(`([+-])\s*0*(\d+)Kg`) // Format contoh: "+ 000010Kg"
	match := re.FindStringSubmatch(data)

	if len(match) == 3 {
		sign := match[1]   // + atau -
		number := match[2] // Angka tanpa leading zero

		// Hapus tanda '+' jika ada
		if sign == "+" {
			return number
		}
		return sign + number
	}

	return ""
}

// Fungsi untuk menambahkan klien SSE baru
func addClient(ctx context.Context) *SSEClient {
	client := &SSEClient{
		ctx:  ctx,
		data: make(chan string, 1),
	}

	clients.Lock()
	clients.list = append(clients.list, client)
	clients.Unlock()

	return client
}

// Fungsi untuk menghapus klien jika mereka disconnect
func removeClient(client *SSEClient) {
	clients.Lock()
	defer clients.Unlock()

	for i, c := range clients.list {
		if c == client {
			clients.list = append(clients.list[:i], clients.list[i+1:]...)
			close(c.data)
			break
		}
	}

	// Tutup koneksi jika tidak ada klien yang tersisa
	if len(clients.list) == 0 {
		portMutex.Lock()
		if port != nil {
			port.Close()
			port = nil
			fmt.Println("Serial port ditutup karena tidak ada klien aktif.")
		}
		portMutex.Unlock()
	}
}

// Fungsi untuk broadcast data ke semua klien SSE dan menyimpan data terakhir
func broadcast(data string) {
	clients.Lock()
	lastWeight = data // Simpan data terakhir
	clients.Unlock()

	for _, client := range clients.list {
		select {
		case client.data <- data:
		case <-client.ctx.Done():
			removeClient(client)
		}
	}
}

func main() {
	e := echo.New()
	e.Use(middleware.Logger())

	// Endpoint untuk mendapatkan berat terakhir dalam format JSON
	e.GET("/weight", func(c echo.Context) error {
		clients.Lock()
		defer clients.Unlock()

		if lastWeight == "" {
			return c.JSON(404, map[string]string{"error": "Data belum tersedia"})
		}
		return c.JSON(200, map[string]string{"weight": lastWeight})
	})

	// Endpoint SSE untuk streaming berat
	e.GET("/weight/stream", func(c echo.Context) error {
		// Buka koneksi serial jika belum terbuka
		portMutex.Lock()
		if port == nil {
			config := &serial.Config{
				Name:     "/dev/ttyUSB0", //COM3, COM4, /dev/ttyUSB0
				Baud:     9600,
				Size:     7,
				Parity:   serial.ParityEven,
				StopBits: serial.Stop1,
			}

			var err error
			port, err = serial.OpenPort(config)
			if err != nil {
				portMutex.Unlock()
				log.Printf("Gagal membuka port serial: %v", err)
				return c.JSON(500, map[string]string{"error": "Gagal membuka koneksi ke timbangan"})
			}
			fmt.Println("Serial port dibuka.")
			go readWeight() // Mulai membaca data
		}
		portMutex.Unlock()

		// Set Header untuk SSE
		c.Response().Header().Set("Content-Type", "text/event-stream")
		c.Response().Header().Set("Cache-Control", "no-cache")
		c.Response().Header().Set("Connection", "keep-alive")

		// Kirim ping untuk memastikan header langsung dikirim
		_, err := c.Response().Write([]byte(":\n\n"))
		if err != nil {
			return err
		}
		c.Response().Flush()

		client := addClient(c.Request().Context())
		defer removeClient(client)

		for {
			select {
			case data := <-client.data:
				jsonData := fmt.Sprintf(`{"weight": "%s"}`, data)
				event := fmt.Sprintf("data: %s\n\n", jsonData)
				_, err := c.Response().Write([]byte(event))
				if err != nil {
					log.Printf("Gagal menulis ke SSE: %v", err)
					return err
				}
				c.Response().Flush() // Pastikan data langsung dikirim ke klien
			case <-c.Request().Context().Done():
				return nil
			}
		}
	})

	e.Logger.Fatal(e.Start(":8080"))
}
