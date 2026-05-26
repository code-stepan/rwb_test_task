package models

import "time"

type SearchEvent struct {
	EventID         string    `json:"event_id"`         // уникальный ID для идемпотентности
	Timestamp       time.Time `json:"timestamp"`        // время события (не время ingestion)
	QueryNormalized string    `json:"query_normalized"` // предварительно очищенный запрос
	UserID          string    `json:"user_id"`          // ID пользователя
	SessionID       string    `json:"session_id"`       // ID сессии
	IPHash          string    `json:"ip_hash"`          // хеш IP (без PII)
	DeviceType      string    `json:"device_type"`      // тип устройства (mobile/desktop)
	Platform        string    `json:"platform"`         // платформа (ios/android/web)
	Source          string    `json:"source"`           // источник (main_search/catalog)
}
