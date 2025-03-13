package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/joho/godotenv"
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
	lastWeight string
)

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

func parseWeight(data string) string {
	re := regexp.MustCompile(`([+-])\s*0*(\d+)Kg`)
	match := re.FindStringSubmatch(data)

	if len(match) == 3 {
		sign := match[1]
		number := match[2]
		if sign == "+" {
			return number
		}
		return sign + number
	}
	return ""
}

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

func broadcast(data string) {
	clients.Lock()
	lastWeight = data
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
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("Gagal membaca file .env, menggunakan nilai default")
	}

	serialPort := os.Getenv("SERIAL_PORT")
	if serialPort == "" {
		serialPort = "COM3" // Default port jika tidak ada di .env
	}

	portConfig := &serial.Config{
		Name:     serialPort,
		Baud:     9600,
		Size:     7,
		Parity:   serial.ParityEven,
		StopBits: serial.Stop1,
	}

	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/weight", func(c echo.Context) error {
		clients.Lock()
		defer clients.Unlock()

		if lastWeight == "" {
			return c.JSON(404, map[string]string{"error": "Data belum tersedia"})
		}
		return c.JSON(200, map[string]string{"weight": lastWeight})
	})

	e.GET("/weight/stream", func(c echo.Context) error {
		portMutex.Lock()
		if port == nil {
			var err error
			port, err = serial.OpenPort(portConfig)
			if err != nil {
				portMutex.Unlock()
				log.Printf("Gagal membuka port serial: %v", err)
				return c.JSON(500, map[string]string{"error": "Gagal membuka koneksi ke timbangan"})
			}
			fmt.Println("Serial port dibuka.")
			go readWeight()
		}
		portMutex.Unlock()

		c.Response().Header().Set("Content-Type", "text/event-stream")
		c.Response().Header().Set("Cache-Control", "no-cache")
		c.Response().Header().Set("Connection", "keep-alive")
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
				c.Response().Flush()
			case <-c.Request().Context().Done():
				return nil
			}
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	e.Logger.Fatal(e.Start(":" + port))
}
