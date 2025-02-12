package main

import (
	"fmt"
	"log"
	"regexp"

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
			rawData := string(buffer[:n])
			weight := parseWeight(rawData) // Ambil angka dengan tanda

			// Hanya cetak jika data berubah dan tidak kosong
			if weight != "" && weight != prevData {
				fmt.Printf("Filtered Weight  : %s\n", weight)
				fmt.Println("----------------------------------------")
				prevData = weight // Simpan data untuk perbandingan berikutnya
			}
		}
	}
}

// Fungsi untuk mengekstrak berat dari data mentah
func parseWeight(data string) string {
	re := regexp.MustCompile(`([+-])\s*0*(\d+)Kg`) // Cari pola "+ 000010Kg" atau "- 000160Kg"
	match := re.FindStringSubmatch(data)

	if len(match) == 3 {
		sign := match[1]   // + atau -
		number := match[2] // Angka tanpa leading zero
		return sign + number
	}

	return ""
}
