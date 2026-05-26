package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	KafkaBrokers   string        // брокеры Kafka
	KafkaTopic     string        // топик для чтения
	KafkaGroupID   string        // consumer group
	HTTPPort       string        // порт HTTP API
	WindowSlots    int           // количество слотов в окне
	SlotDuration   time.Duration // длительность одного слота
	TopN           int           // сколько трендов возвращать
	TopK           int           // размер hot heap
	RecalcInterval time.Duration // интервал пересчёта трендов
	RateLimitIP    float64       // лимит запросов с IP (в секунду)
	RateLimitUser  float64       // лимит запросов от пользователя (в секунду)
	RateLimitBurst float64       // burst для token bucket
	QuarantineDur  time.Duration // длительность карантина при всплеске
}

func Load() *Config {
	return &Config{
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "search_log"),
		KafkaGroupID:   getEnv("KAFKA_GROUP_ID", "trending-search-consumer"),
		HTTPPort:       getEnv("HTTP_PORT", "8080"),
		WindowSlots:    getEnvInt("WINDOW_SLOTS", 30),
		SlotDuration:   getEnvDuration("SLOT_DURATION_SEC", 10) * time.Second,
		TopN:           getEnvInt("TOP_N", 100),
		TopK:           getEnvInt("TOP_K", 300),
		RecalcInterval: getEnvDuration("RECALC_INTERVAL_MS", 500) * time.Millisecond,
		RateLimitIP:    getEnvFloat("RATE_LIMIT_IP", 30.0/60.0),
		RateLimitUser:  getEnvFloat("RATE_LIMIT_USER", 10.0/60.0),
		RateLimitBurst: getEnvFloat("RATE_LIMIT_BURST", 5.0),
		QuarantineDur:  getEnvDuration("QUARANTINE_SEC", 60) * time.Second,
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return i
}

func getEnvFloat(key string, defaultVal float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultVal
	}
	return f
}

func getEnvDuration(key string, defaultVal int) time.Duration {
	return time.Duration(getEnvInt(key, defaultVal))
}
