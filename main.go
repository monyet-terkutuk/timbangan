package main

import (
	"fmt"
	"log"
	"strings"
	"unicode"

	"github.com/tarm/serial"
)

func main() {
	config := &serial.Config{
		Name:     "/dev/ttyUSB0",
		Baud:     9600,
		Size:     7,
		Parity:   serial.ParityEven,
		StopBits: serial.Stop1,
	}

	port, err := serial.OpenPort(config)
	if err != nil {
		log.Fatalf("Gagal membuka port serial: %v", err)
	}
	defer port.Close()

	fmt.Println("Menunggu data dari timbangan...")

	buffer := make([]byte, 128)
	var prevData string // Variabel untuk menyimpan data sebelumnya

	for {
		n, err := port.Read(buffer)
		if err != nil {
			log.Printf("Gagal membaca data: %v", err)
			continue
		}

		if n > 0 {
			rawData := buffer[:n]
			cleanData := filterPrintable(rawData)
			cleanData = strings.TrimSpace(cleanData) // Hapus spasi kosong

			// Hanya cetak jika data berubah
			if cleanData != "" && cleanData != prevData {
				fmt.Printf("Raw Data (Hex)   : %X\n", rawData)
				fmt.Printf("Filtered String  : %s\n", cleanData)
				fmt.Println("----------------------------------------")
				prevData = cleanData // Simpan data sebagai referensi untuk loop berikutnya
			}
		}
	}
}

// Fungsi untuk membuang karakter non-printable
func filterPrintable(data []byte) string {
	var result []rune
	for _, b := range data {
		if unicode.IsPrint(rune(b)) {
			result = append(result, rune(b))
		}
	}
	return string(result)
}
