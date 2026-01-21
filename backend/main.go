package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
	"github.com/joho/godotenv"
)

// const (
// 	keycloakURL        = "http://localhost:8080"
// 	realm              = "reports-realm"
// 	clickhouseAddr     = "127.0.0.1:9000"
// 	clickhouseDatabase = "airflow"
// 	clickhouseUsername = "airflow"
// 	clickhousePassword = "airflow"
// )

type Row struct {
	UserEmail         string  `json:"user_email"`
	ProsthesisId      string  `json:"prosthesis_id"`
	SignalStrength    float32 `json:"signal_strength"`
	BatteryPercentage int32   `json:"battery_percentage"`
}

func CORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Credentials", "true")
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")

		if r.Method == "OPTIONS" {
			http.Error(w, "No Content", http.StatusNoContent)
			return
		}

		next(w, r)
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}

	router := http.NewServeMux()
	router.HandleFunc("/reports", CORS(CheckToken(func(w http.ResponseWriter, r *http.Request) {
		userEmail := r.Header.Get("User-Email")
		data := GetData(userEmail)

		w.Header().Set("Content-Type", "application/json")

		err := json.NewEncoder(w).Encode(data)

		if err != nil {
			fmt.Fprintf(w, "%s", err.Error())
		}
	})))

	http.ListenAndServe(":8000", router)
}

func CheckToken(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keycloakURL := os.Getenv("KEYCLOAK_URL")
		realm := os.Getenv("REALM")

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header is missing", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")

		ctx, cancel := context.WithCancel(context.Background())
		options := keyfunc.Options{
			Ctx: ctx,
			RefreshErrorHandler: func(err error) {
				log.Printf("There was an error with the jwt.Keyfunc\nError: %s", err.Error())
			},
			RefreshInterval:   time.Hour,
			RefreshRateLimit:  time.Minute * 5,
			RefreshTimeout:    time.Second * 10,
			RefreshUnknownKID: true,
		}
		jwks, err := keyfunc.Get(fmt.Sprintf("%s/realms/%s/protocol/openid-connect/certs", keycloakURL, realm), options)
		if err != nil {
			log.Printf("Failed to create JWKS from resource at the given URL.\nError: %s", err.Error())
		}

		token1, err := jwt.Parse(token, jwks.Keyfunc)
		if err != nil {
			panic(err)
		}
		claims := token1.Claims.(jwt.MapClaims)

		r.Header.Set("User-Email", claims["email"].(string))

		cancel()

		jwks.EndBackground()

		next.ServeHTTP(w, r)
	})
}

func GetData(userEmail string) []Row {
	var data []Row

	conn, err := connect()
	if err != nil {
		log.Print(err)
		return data
	}

	ctx := context.Background()
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT user_email, prosthesis_id, signal_strength, battery_percentage FROM airflow.reports WHERE user_email = '%s'", userEmail))
	if err != nil {
		log.Print(err)
		return data
	}
	defer rows.Close()

	for rows.Next() {
		var userEmail, prosthesisId string
		var signalStrength float32
		var batteryPercentage int32

		var rd Row
		if err := rows.Scan(&userEmail, &prosthesisId, &signalStrength, &batteryPercentage); err != nil {
			log.Print(err)
			return data
		}

		rd.UserEmail = userEmail
		rd.ProsthesisId = prosthesisId
		rd.SignalStrength = signalStrength
		rd.BatteryPercentage = batteryPercentage

		data = append(data, rd)
	}

	if err := rows.Err(); err != nil {
		log.Print(err)
		return data
	}

	return data
}

func connect() (driver.Conn, error) {

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{os.Getenv("CLICKHOUSE_ADDR")},
			Auth: clickhouse.Auth{
				Database: os.Getenv("CLICKHOUSE_DATABASE"),
				Username: os.Getenv("CLICKHOUSE_USERNAME"),
				Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
